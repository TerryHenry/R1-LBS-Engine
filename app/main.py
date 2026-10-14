import asyncio,json,math,random,time,os,threading,zlib
from pathlib import Path
from fastapi import FastAPI,WebSocket
from fastapi.responses import HTMLResponse, Response
from app.integrations import RuckusOneClient
from app.lbs_mqtt_server import MosquittoLBSBroker
from app.location_engine import estimate_position,smooth,norm_mac,_lookup_ap

BASE=Path(__file__).resolve().parent.parent
RAW_LOG=BASE/'logs'/'lbs_raw.jsonl'
RAW_LOG.parent.mkdir(exist_ok=True)
# Persistent configuration shared by all revisions.
CONFIG_DIR=Path.home()/'AppData'/'Local'/'RuckusLocationServices'
CONFIG_FILE=CONFIG_DIR/'settings.json'
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

def _legacy_candidates():
    out=[BASE/'config'/'settings.json', Path.home()/'AppData'/'Roaming'/'RuckusLocationServices'/'settings.json', Path(os.environ.get('PROGRAMDATA','C:/ProgramData'))/'RuckusLocationServices'/'settings.json']
    for root in [Path.home()/'Downloads',Path.home()/'Desktop',Path.home()/'Documents']:
        try:
            if root.exists():
                out.extend(root.glob('RUCKUS-Location-Services-v*/config/settings.json'))
                out.extend(root.glob('ruckus-location-v*/config/settings.json'))
        except Exception: pass
    seen=set(); result=[]
    for p in out:
        k=str(p).lower()
        if k not in seen and p != CONFIG_FILE: seen.add(k); result.append(p)
    return result

def _normalize_config(c):
    if not isinstance(c,dict): c={}
    r=c.get('ruckus_one')
    if not isinstance(r,dict): r={}; c['ruckus_one']=r
    r.setdefault('base_url','https://api.ruckus.cloud'); r.setdefault('auth_url','https://ruckus.cloud')
    c.setdefault('lbs_server',{'bind_address':'0.0.0.0','advertised_ip':'','port':8883,'lbs_server_venue_name':'','username':'','password':'','tls':True,'topic':'#'})
    c.setdefault('location_engine',{'enabled':True,'reference_rssi':-45,'path_loss_exponent':2.2,'smoothing':0.35,'live_seconds':30})
    return c

def save_config(c):
    c=_normalize_config(c); CONFIG_DIR.mkdir(parents=True,exist_ok=True)
    tmp=CONFIG_FILE.with_suffix('.tmp'); tmp.write_text(json.dumps(c,indent=2),encoding='utf-8'); tmp.replace(CONFIG_FILE)

def load_config():
    candidates=[CONFIG_FILE]+_legacy_candidates(); fallback={}
    for path in candidates:
        try:
            if not path.exists(): continue
            c=_normalize_config(json.loads(path.read_text(encoding='utf-8'))); r=c.get('ruckus_one',{})
            if not fallback: fallback=c
            if str(r.get('tenant_id') or '').strip() and str(r.get('client_id') or '').strip():
                if path != CONFIG_FILE: save_config(c)
                return c
        except Exception: continue
    return fallback

def r1_client():
    c=load_config().get('ruckus_one',{}); return RuckusOneClient(c.get('base_url','https://api.ruckus.cloud'),c.get('auth_url','https://ruckus.cloud'),c.get('tenant_id',''),c.get('client_id',''),c.get('client_secret',''))
def require_r1():
    c=load_config().get('ruckus_one',{}); return bool(c.get('tenant_id') and c.get('client_id') and c.get('client_secret'))

def pick(obj,*names,default=None):
    if not isinstance(obj,dict): return default
    for n in names:
        if n in obj and obj[n] is not None:return obj[n]
    return default

def data_list(body):
    if isinstance(body,list): return body
    if isinstance(body,dict):
        d=body.get('data')
        if isinstance(d,list): return d
        for k in ('items','results','records'):
            if isinstance(body.get(k),list): return body[k]
    return []

def query_payload(page=1,size=100): return {'page':page,'pageSize':size}

app=FastAPI(title='RUCKUS Location Services v1.7')
connections=set(); lbs_receiver=None
location_observations={}
location_estimates={}
LAST_LOC_FILE=BASE/'logs'/'last_locations.json'
_last_loc_save=0.0
def _load_last_locations():
    try:
        d=json.loads(LAST_LOC_FILE.read_text(encoding='utf-8'))
        for k,v in d.items():
            if isinstance(v,dict) and v.get('xPercent') is not None: location_estimates[k]=v
    except Exception: pass
def _save_last_locations(now):
    global _last_loc_save
    if now-_last_loc_save<30: return
    _last_loc_save=now
    try:
        tmp=LAST_LOC_FILE.with_suffix('.tmp'); tmp.write_text(json.dumps(location_estimates),encoding='utf-8'); tmp.replace(LAST_LOC_FILE)
    except Exception: pass
_load_last_locations()
location_stable={}   # client -> {ap: (accepted_rssi, last_seen)}
location_ap_seen={}  # client -> {ap: (accepted_rssi, reading_ts)}  last RSSI per AP, kept after the client goes quiet
RSSI_DEADBAND_DB=3.0
RSSI_AVG_WINDOW=60.0    # seconds of readings averaged per AP (bands and fading are smoothed out)
AP_CARRY_SECONDS=180.0  # an AP keeps contributing its last accepted RSSI this long after it goes quiet
LIVE_SECONDS=30.0       # default: a client is "live" only if >=3 APs were heard within this window
LIVE_MIN,LIVE_MAX=5.0,300.0

def live_window(c):
    """The configurable 'seen within N seconds' window (Location settings -> live_seconds)."""
    try: v=float(c.get('live_seconds') or LIVE_SECONDS)
    except Exception: v=LIVE_SECONDS
    return max(LIVE_MIN,min(LIVE_MAX,v))
HOLD_PCT=5.0            # new estimates closer than this (map %) to the published spot are ignored
MOVE_CONFIRM_POLLS=3    # an estimate outside the hold radius must repeat this many polls before the dot moves
location_pending={}     # client -> consecutive polls with an estimate outside the hold radius

def stabilize(cid, obs, now, window=RSSI_AVG_WINDOW, carry=AP_CARRY_SECONDS):
    """Per-AP RSSI: 60 s average, 3 dB deadband, and the AP stays in the set for
    3 minutes after its last reading so a weak AP blinking in and out cannot change
    the geometry (and therefore the position) every few seconds."""
    per={}
    for o in obs:
        try:
            ts=float(o.get('timestamp') or now); r=float(o.get('rssi'))
        except Exception: continue
        if now-ts>window: continue
        ap=norm_mac(o.get('ap_id') or '')
        e=per.setdefault(ap,[0.0,0,0.0]); e[0]+=r; e[1]+=1; e[2]=max(e[2],ts)
    st=location_stable.setdefault(cid,{})
    for ap,(tot,n,ts) in per.items():
        avg=tot/n; prev=st.get(ap)
        acc=avg if (prev is None or abs(avg-prev[0])>=RSSI_DEADBAND_DB) else prev[0]
        st[ap]=(acc,ts)
        location_ap_seen.setdefault(cid,{})[ap]=(acc,ts)
    for ap in [a for a,v in st.items() if now-v[1]>carry]: del st[ap]
    return [{'ap_id':ap,'rssi':v[0],'timestamp':v[1]} for ap,v in st.items()]

location_near_ap={}    # client -> AP key currently used for the near-AP marker
NEAR_SWITCH_DB=6.0      # another AP must be this much stronger before the near-AP marker changes AP

def near_ap_estimate(cid, obs, now, stranger=False, live=LIVE_SECONDS):
    """Single-AP fallback: place the client next to the strongest positioned AP that heard it
    in the last 30 s. The offset (distance grows as RSSI falls, angle from the MAC) is
    deterministic, so the marker never wanders and devices on one AP do not stack."""
    cand=[]
    for o in obs:
        if now-o['timestamp']>live: continue
        p=_lookup_ap(o['ap_id'],location_ap_positions)
        if p is not None: cand.append((o['rssi'],o['ap_id'],p))
    if not cand: return None
    cand.sort(reverse=True)
    best=cand[0]; cur=location_near_ap.get(cid)
    if cur:
        same=[x for x in cand if x[1]==cur]
        if same and best[0]-same[0][0]<NEAR_SWITCH_DB: best=same[0]
    location_near_ap[cid]=best[1]
    rssi,ap,(ax,ay)=best
    dist=2.0+(min(max(-rssi,40),90)-40)*0.12          # 2% (strong) .. 8% (weak)
    ang=(zlib.crc32(cid.encode())%3600)/3600*2*math.pi
    x=max(0,min(100,ax+dist*math.cos(ang))); y=max(0,min(100,ay+dist*math.sin(ang)))
    return {'xPercent':round(x,2),'yPercent':round(y,2),'accuracy':round(dist+4,1),'confidence':25,'method':'Near AP','kind':'near',
            'apId':ap,'apCount':len({c[1] for c in cand}),'observations':[{'apId':a,'rssi':r} for r,a,_ in cand],'timestamp':now,
            **({'stranger':True} if stranger else {})}

STRANGER_MIN_RSSI=-70      # unlisted devices only get a near-AP marker when this strong...
STRANGER_MIN_READINGS=10   # ...and heard this often in the last 2 minutes (filters passing random MACs)
STRANGER_MAX=200           # never return more than this many (strongest first)

def between_estimate(cid, live, now, c):
    """Two-AP estimate: a point on the line between the two strongest positioned APs, nearer the stronger one.
    A client heard by only two APs lies somewhere on a curve, so this is approximate; it is labelled as such."""
    best={}
    for o in live:
        k=norm_mac(o['ap_id'])
        if k not in best or o['rssi']>best[k]['rssi']: best[k]=o
    top=sorted(best.values(),key=lambda o:-o['rssi'])[:2]
    if len(top)<2: return None
    p1=_lookup_ap(top[0]['ap_id'],location_ap_positions); p2=_lookup_ap(top[1]['ap_id'],location_ap_positions)
    if p1 is None or p2 is None or p1==p2: return None
    ref=float(c.get('reference_rssi',-45)); n=max(float(c.get('path_loss_exponent',2.2) or 2.2),1.0)
    d1=10**((ref-top[0]['rssi'])/(10*n)); d2=10**((ref-top[1]['rssi'])/(10*n))
    t=max(0.15,min(0.85,d1/(d1+d2)))
    x=p1[0]+t*(p2[0]-p1[0]); y=p1[1]+t*(p2[1]-p1[1])
    ids=[norm_mac(top[0]['ap_id']),norm_mac(top[1]['ap_id'])]
    prev=location_estimates.get(cid)
    if prev and prev.get('kind')=='between' and set(prev.get('apIds',[]))==set(ids) and math.hypot(x-prev['xPercent'],y-prev['yPercent'])<3.0:
        x,y=prev['xPercent'],prev['yPercent']            # hold: ignore small changes so the marker stays put
    return {'xPercent':round(x,2),'yPercent':round(y,2),'accuracy':round(math.hypot(p2[0]-p1[0],p2[1]-p1[1])/2,1),'confidence':35,
            'method':'Between two APs','kind':'between','apIds':ids,'apId':ids[0],'apCount':2,
            'observations':[{'apId':o['ap_id'],'rssi':o['rssi']} for o in top],'timestamp':now}

def locate_client(cid, obs, now, c, allow_near=True, strangers=False):
    """Locate, then note APs that heard the client but have no floor position (their readings cannot be used)."""
    est=_locate_client(cid,obs,now,c,allow_near,strangers)
    if est:
        live_s=live_window(c)
        heard={norm_mac(o['ap_id']) for o in obs if now-float(o.get('timestamp') or now)<=live_s}
        unplaced=sorted(a for a in heard if _lookup_ap(a,location_ap_positions) is None)
        if unplaced: est['unplacedAps']=unplaced
    return est

def _locate_client(cid, obs, now, c, allow_near=True, strangers=False):
    """stabilize -> (3+ APs: fix | 2 APs: between | 1 AP: near) -> hold/confirm -> smooth."""
    recent=sum(1 for o in obs if now-float(o.get('timestamp') or now)<=120)
    live_s=live_window(c)
    window=max(RSSI_AVG_WINDOW,live_s); carry=max(AP_CARRY_SECONDS,live_s+30)   # averaging/carry never shorter than the live window
    obs=stabilize(cid,obs,now,window,carry)
    live=[o for o in obs if now-o['timestamp']<=live_s and _lookup_ap(o['ap_id'],location_ap_positions) is not None]
    n_live=len({norm_mac(o['ap_id']) for o in live})
    def fallback():
        if n_live>=2:
            e=between_estimate(cid,live,now,c)
            if e: return e
        if allow_near: return near_ap_estimate(cid,obs,now,live=live_s)
        if strangers and n_live>=1 and recent>=STRANGER_MIN_READINGS and max(o['rssi'] for o in live)>=STRANGER_MIN_RSSI:
            return near_ap_estimate(cid,obs,now,stranger=True,live=live_s)
        return None
    # Live fix only if enough distinct positioned APs were heard recently; carried APs then help the geometry.
    if n_live<3: return fallback()
    est=estimate_position(obs,location_ap_positions,float(c.get('reference_rssi',-45)),float(c.get('path_loss_exponent',2.2)),max_age=carry+5)
    if not est: return fallback()
    prev=location_estimates.get(cid)
    if prev and prev.get('kind') in ('near','between'): prev=None   # a real fix replaces an approximate marker outright
    if prev:
        d=math.hypot(est['xPercent']-prev['xPercent'],est['yPercent']-prev['yPercent'])
        if d<HOLD_PCT:
            location_pending[cid]=0
            held=dict(est); held['xPercent']=prev['xPercent']; held['yPercent']=prev['yPercent']
            return held
        location_pending[cid]=location_pending.get(cid,0)+1
        if location_pending[cid]<MOVE_CONFIRM_POLLS:
            held=dict(est); held['xPercent']=prev['xPercent']; held['yPercent']=prev['yPercent']
            return held
    location_pending[cid]=0
    return smooth(prev,est,float(c.get('smoothing',0.35)))
location_ap_positions={}
location_meta={'lastTelemetry':None,'observationCount':0,'receiverError':None}
# simulator retained
APs=[{'id':f'AP-0{i}','x':x,'y':y,'floor':2} for i,(x,y) in enumerate([(12,15),(82,15),(12,82),(82,82),(50,48)],1)]
clients=[{'id':'c1','name':'Client-01','mac':'02:00:00:00:00:01','x':48,'y':38,'status':'Moving','wlan':'Corp-WiFi','ap':'AP-05'}, {'id':'c2','name':'Client-17','mac':'02:00:00:00:00:17','x':58,'y':62,'status':'Moving','wlan':'Corp-WiFi','ap':'AP-05'}, {'id':'c3','name':'Guest-04','mac':'02:00:00:00:00:04','x':72,'y':42,'status':'Stationary','wlan':'Guest','ap':'AP-02'}, {'id':'c4','name':'Scanner-12','mac':'02:00:00:00:00:12','x':27,'y':67,'status':'Moving','wlan':'IoT','ap':'AP-03'}, {'id':'c5','name':'Laptop-23','mac':'02:00:00:00:00:23','x':35,'y':25,'status':'Moving','wlan':'Corp-WiFi','ap':'AP-01'}]

def nearest_ap(x,y): return min(APs,key=lambda a:(a['x']-x)**2+(a['y']-y)**2)['id']
def sim_snapshot():
    now=time.time(); out=[]
    for c in clients:
        ap=nearest_ap(c['x'],c['y']); ds={a['id']:math.hypot(a['x']-c['x'],a['y']-c['y']) for a in APs}; c['ap']=ap
        out.append({**c,'rssi':round(-38-20*math.log10(max(ds[ap],1))+random.uniform(-2,2)),'accuracy':round(3.5+random.random()*2.5,1),'confidence':round(78+random.random()*18),'timestamp':now})
    return out
def simulate():
    for c in clients:
        if c['status']=='Moving':
            a=random.random()*2*math.pi; s=random.uniform(.8,2); c['x']=max(8,min(92,c['x']+math.cos(a)*s)); c['y']=max(8,min(92,c['y']+math.sin(a)*s))

@app.get('/api/health')
def health(): return {'status':'ok','version':'1.6','mode':'ruckus_one','lbs_receiver':bool(lbs_receiver and lbs_receiver.running)}
@app.get('/api/config')
def config():
    c=load_config(); r=c.get('ruckus_one',{}) if isinstance(c.get('ruckus_one',{}),dict) else {}
    return {'ok':True,'ruckus_one':{'base_url':r.get('base_url','https://api.ruckus.cloud'),'auth_url':r.get('auth_url','https://ruckus.cloud'),'tenant_id':r.get('tenant_id',''),'client_id':r.get('client_id',''),'client_secret_set':bool(r.get('client_secret'))}}
@app.post('/api/config/ruckus-one')
def configure(payload:dict):
    c=load_config(); r=c.get('ruckus_one',{}) if isinstance(c.get('ruckus_one',{}),dict) else {}
    tenant=str(payload.get('tenant_id','')).strip(); client=str(payload.get('client_id','')).strip(); secret=str(payload.get('client_secret','')).strip()
    missing=[]
    if not tenant: missing.append('tenant_id')
    if not client: missing.append('client_id')
    if not secret and not str(r.get('client_secret','')).strip(): missing.append('client_secret')
    if missing: return {'ok':False,'error':'Missing: '+', '.join(missing)}
    c['ruckus_one']={'base_url':str(payload.get('base_url','https://api.ruckus.cloud')).strip().rstrip('/'),'auth_url':str(payload.get('auth_url','https://ruckus.cloud')).strip().rstrip('/'),'tenant_id':tenant,'client_id':client,'client_secret':secret or r.get('client_secret','')}
    save_config(c); return {'ok':True,'config_file':str(CONFIG_FILE)}
@app.post('/api/config/recover')
def recover_config():
    for path in _legacy_candidates():
        try:
            if not path.exists(): continue
            c=_normalize_config(json.loads(path.read_text(encoding='utf-8'))); r=c.get('ruckus_one',{})
            if r.get('tenant_id') and r.get('client_id') and r.get('client_secret'):
                save_config(c); return {'ok':True,'tenant_id':r['tenant_id'],'client_id':r['client_id'],'secret_set':True,'source':str(path)}
        except Exception: continue
    return {'ok':False,'error':'No complete previous RUCKUS One configuration was found.'}
@app.get('/api/config/lbs')
def lbs_config():
    c=load_config().get('lbs_server',{})
    return {'ok':True,'lbs_server':{
        'bind_address':c.get('bind_address','0.0.0.0'),
        'advertised_ip':c.get('advertised_ip',''),
        'port':int(c.get('port',8883) or 8883),
        'lbs_server_venue_name':c.get('lbs_server_venue_name',''),
        'username':c.get('username',''),
        'password_set':bool(c.get('password')),
        'tls':True,
        'topic':c.get('topic','#'),
        'running':bool(lbs_receiver and lbs_receiver.running),
        'broker_ready':bool(getattr(lbs_receiver,'broker_ready',False) if lbs_receiver else False),
        'local_connected':bool(getattr(lbs_receiver,'local_connected',False) if lbs_receiver else False),
        'connected_clients':int(getattr(lbs_receiver,'connected_clients',0) if lbs_receiver else 0),
        'connection_count':int(getattr(lbs_receiver,'connection_count',0) if lbs_receiver else 0),
        'last_connection':getattr(lbs_receiver,'last_connection',None) if lbs_receiver else None,
        'last_disconnect':getattr(lbs_receiver,'last_disconnect',None) if lbs_receiver else None,
        'last_stage':getattr(lbs_receiver,'last_stage','stopped') if lbs_receiver else 'stopped',
        'last_connect_result':getattr(lbs_receiver,'last_connect_result',None) if lbs_receiver else None,
        'message_count':int(getattr(lbs_receiver,'message_count',0) if lbs_receiver else 0),
        'last_message':getattr(lbs_receiver,'last_message',None) if lbs_receiver else None,
        'tls_handshake_failures':int(getattr(lbs_receiver,'tls_handshake_failures',0) if lbs_receiver else 0),
        'last_tls_failure':getattr(lbs_receiver,'last_tls_failure',None) if lbs_receiver else None,
        'last_peer':getattr(lbs_receiver,'last_peer',None) if lbs_receiver else None,
        'last_event':getattr(lbs_receiver,'last_event',None) if lbs_receiver else None,
        'last_topics':getattr(lbs_receiver,'last_topics',[]) if lbs_receiver else [],
        'mosquitto_found':bool(lbs_receiver and lbs_receiver.find_executable()) if lbs_receiver else False,
        'error':getattr(lbs_receiver,'last_error',None) if lbs_receiver else location_meta.get('receiverError')
    }}

@app.post('/api/config/lbs')
def configure_lbs(payload:dict):
    global lbs_receiver
    c=load_config(); old=c.get('lbs_server',{}) if isinstance(c.get('lbs_server',{}),dict) else {}
    try: port=int(payload.get('port',old.get('port',8883)))
    except Exception: return {'ok':False,'error':'Port must be a number.'}
    if not 1 <= port <= 65535: return {'ok':False,'error':'Port must be between 1 and 65535.'}
    password=str(payload.get('password','')).strip() or str(old.get('password',''))
    venue_name=str(payload.get('lbs_server_venue_name',old.get('lbs_server_venue_name',''))).strip()
    if not venue_name: return {'ok':False,'error':'Enter the LBS Server Venue Name exactly as configured in RUCKUS One.'}
    if not password: return {'ok':False,'error':'Enter the LBS password used in the RUCKUS One LBS profile.'}
    if not 8 <= len(password) <= 63: return {'ok':False,'error':'LBS password must be 8-63 characters.'}
    c['lbs_server']={
        'bind_address':str(payload.get('bind_address',old.get('bind_address','0.0.0.0'))).strip() or '0.0.0.0',
        'advertised_ip':str(payload.get('advertised_ip',old.get('advertised_ip',''))).strip(),
        'port':port,
        'lbs_server_venue_name':venue_name,
        'username':'',
        'password':password,
        'tls':True,
        'topic':str(payload.get('topic',old.get('topic','#'))).strip() or '#'
    }
    save_config(c)
    return restart_lbs_receiver()

_obs_lock=threading.Lock()

def _on_lbs_observations(obs):
    global location_meta
    now=time.time(); location_meta['lastTelemetry']=now; location_meta['observationCount'] += len(obs)
    with _obs_lock:
        for o in obs:
            cid=o.get('client_id')
            if not cid: continue
            ap_id=o.get('ap_id') or o.get('ap_mac')
            # Use receive time: AP clocks/epoch in PAR can be skewed.
            location_observations.setdefault(cid,[]).append({'ap_id':ap_id,'rssi':o.get('rssi'),'snr':o.get('snr'),'timestamp':now,'ap_timestamp':o.get('timestamp'),'band':o.get('band'),'rssi_chains':o.get('rssi_chains',[])})
            location_observations[cid]=location_observations[cid][-600:]
        # Drop clients not heard for 5 minutes so the dict cannot grow forever.
        for k in [k for k,v in location_observations.items() if v and now-v[-1]['timestamp']>LIVE_MAX+30]:
            del location_observations[k]

def restart_lbs_receiver(force=False):
    global lbs_receiver, location_meta
    if lbs_receiver:
        try: lbs_receiver.stop()
        except Exception: pass
        lbs_receiver=None
    c=load_config().get('lbs_server',{})
    try:
        lbs_receiver=MosquittoLBSBroker(
            host=c.get('bind_address','0.0.0.0'),
            port=int(c.get('port',8883)),
            password=c.get('password',''),
            lbs_server_venue_name=c.get('lbs_server_venue_name',''),
            topic=c.get('topic','#'),
            base_dir=BASE,
            raw_log=RAW_LOG,
            on_observations=_on_lbs_observations)
        # The Mosquitto broker is kept running across app restarts so RUCKUS One stays connected.
        # force=True (or changed LBS settings) replaces it.
        if force: lbs_receiver.stop(kill_broker=True)
        result=lbs_receiver.start(); location_meta['receiverError']=None
        return result
    except Exception as e:
        location_meta['receiverError']=str(e); lbs_receiver=None
        return {'ok':False,'error':str(e),'running':False}

def _autostart_lbs():
    c=load_config().get('lbs_server',{})
    if not isinstance(c,dict) or not str(c.get('lbs_server_venue_name','')).strip() or not str(c.get('password','')):
        return  # not configured yet: the user starts it from the LBS settings dialog
    restart_lbs_receiver()

@app.on_event('startup')
async def _startup_lbs():
    # Start the LBS receiver in the background so the UI is available immediately.
    threading.Thread(target=_autostart_lbs,daemon=True).start()

@app.post('/api/lbs/restart')
def lbs_restart(force:bool=False): return restart_lbs_receiver(force)

@app.get('/api/lbs/diagnostics')
def lbs_diagnostics():
    if not lbs_receiver:
        return {'ok':False,'error':'LBS receiver is not running.'}
    return {'ok':True,'diagnostics':lbs_receiver.diagnostics()}

@app.post('/api/config/test')
async def test():
    if not require_r1(): return {'ok':False,'error':'Configure Tenant ID, Client ID and Shared Secret first.'}
    try:
        r=r1_client(); await r.token(); return {'ok':True,'message':'RUCKUS One authentication succeeded.'}
    except Exception as e:return {'ok':False,'error':str(e)}

@app.get('/api/r1/venues')
async def venues():
    try:
        r=r1_client(); resp=await r.post('/venues/query',json=query_payload()); body=resp.json();
        if not resp.is_success:return {'ok':False,'status':resp.status_code,'error':body}
        return {'ok':True,'data':data_list(body),'raw':body}
    except Exception as e:return {'ok':False,'error':str(e)}
@app.get('/api/r1/venues/{venue_id}/floorplans')
async def floorplans(venue_id:str):
    try:
        r=r1_client(); resp=await r.get(f'/venues/{venue_id}/floorplans'); body=resp.json();
        if not resp.is_success:return {'ok':False,'status':resp.status_code,'error':body}
        return {'ok':True,'data':data_list(body) if isinstance(body,dict) else body}
    except Exception as e:return {'ok':False,'error':str(e)}
@app.get('/api/r1/aps')
async def aps(venue_id:str=''):
    try:
        r=r1_client(); payload=query_payload();
        if venue_id: payload['filters']={'venueId':[venue_id]}
        resp=await r.post('/venues/aps/query',json=payload); body=resp.json();
        if not resp.is_success:return {'ok':False,'status':resp.status_code,'error':body}
        return {'ok':True,'data':data_list(body),'raw':body}
    except Exception as e:return {'ok':False,'error':str(e)}
@app.get('/api/r1/clients')
async def r1clients(venue_id:str=''):
    try:
        r=r1_client(); payload=query_payload();
        if venue_id: payload['filters']={'venueId':[venue_id]}
        resp=await r.post('/venues/aps/clients/query',json=payload); body=resp.json();
        if not resp.is_success:return {'ok':False,'status':resp.status_code,'error':body}
        return {'ok':True,'data':data_list(body),'raw':body}
    except Exception as e:return {'ok':False,'error':str(e)}
@app.get('/api/r1/lbs-profiles')
async def lbs_profiles(venue_id:str='', venue_name:str=''):
    try:
        r=r1_client(); resp=await r.post('/lbsServerProfiles/query',json=query_payload()); body=resp.json()
        if not resp.is_success:return {'ok':False,'status':resp.status_code,'error':body}
        all_items=data_list(body)
        def venue_matches(x):
            # RUCKUS One documents venueIds on the LBS profile. Some tenants may
            # return the venue association using a slightly different shape, so
            # also handle common nested forms and the documented venue-name field.
            ids=x.get('venueIds') or x.get('venueIDs') or x.get('venues') or []
            if isinstance(ids,str): ids=[ids]
            if isinstance(ids,dict): ids=list(ids.values())
            vals=[]
            for v in ids:
                if isinstance(v,dict):
                    for k in ('id','venueId','venueID'):
                        if v.get(k) is not None: vals.append(str(v[k]))
                else: vals.append(str(v))
            if venue_id and str(venue_id) in vals: return True
            if venue_name:
                names=[x.get('lbsServerVenueName'),x.get('venueName'),x.get('name')]
                target=str(venue_name).strip().casefold()
                if target and any(str(n).strip().casefold()==target for n in names if n): return True
            return not venue_id and not venue_name
        items=[x for x in all_items if venue_matches(x)]
        connected=sum(1 for x in items if x.get('serverConnected') is True)
        management=sum(1 for x in items if x.get('managementConnected') is True)
        return {'ok':True,'data':items,'totalProfiles':len(all_items),'venueProfiles':len(items),'connected':connected,'managementConnected':management,'debugFields':[sorted(x.keys()) for x in all_items]}
    except Exception as e:return {'ok':False,'error':str(e)}

@app.get('/api/r1/ap-position')
async def ap_position(venue_id:str,floorplan_id:str,serial_number:str):
    try:
        r=r1_client(); resp=await r.get(f'/venues/{venue_id}/floorplans/{floorplan_id}/aps/{serial_number}/floorPositions');
        body=resp.json(); return {'ok':resp.is_success,'status':resp.status_code,'data':body}
    except Exception as e:return {'ok':False,'error':str(e)}
@app.get('/api/r1/floorplan-image')
async def floorplan_image(venue_id:str, image_id:str):
    """Resolve a RUCKUS One floor-plan imageId to a signed URL, then proxy the image locally."""
    try:
        r=r1_client()
        sr=await r.get(f'/venues/{venue_id}/signurls/{image_id}/urls')
        body=sr.json()
        if not sr.is_success:
            return {'ok':False,'status':sr.status_code,'error':body}
        signed=body.get('signedUrl') if isinstance(body,dict) else None
        if not signed:
            return {'ok':False,'error':'RUCKUS One did not return a signedUrl for this floor-plan image.'}
        import httpx
        async with httpx.AsyncClient(timeout=60,follow_redirects=True) as c:
            ir=await c.get(signed)
        if not ir.is_success:
            return {'ok':False,'status':ir.status_code,'error':'Unable to download the signed floor-plan image.'}
        media=ir.headers.get('content-type','image/png')
        return Response(content=ir.content,media_type=media,headers={'Cache-Control':'private, max-age=300'})
    except Exception as e:
        return {'ok':False,'error':str(e)}

_ap_pos_refreshed={}
AP_POS_REFRESH_SECONDS=300

@app.get('/api/r1/location')
async def r1_location(venue_id:str='', floorplan_id:str='', macs:str='', strangers:bool=False):
    # Refresh AP positions from the selected floorplan (cached for 5 min: AP positions rarely change and
    # this is one REST call per AP), then estimate positions from LBS observations.
    if venue_id and floorplan_id and time.time()-_ap_pos_refreshed.get((venue_id,floorplan_id),0)>=AP_POS_REFRESH_SECONDS:
        _ap_pos_refreshed[(venue_id,floorplan_id)]=time.time()
        try:
            r=r1_client(); ar=await r.post('/venues/aps/query',json={'page':1,'pageSize':100,'filters':{'venueId':[venue_id]}})
            if ar.is_success:
                for a in data_list(ar.json()):
                    serial=pick(a,'serialNumber');
                    if not serial: continue
                    try:
                        pr=await r.get(f'/venues/{venue_id}/floorplans/{floorplan_id}/aps/{serial}/floorPositions')
                        if pr.is_success:
                            pb=pr.json(); pos=pb.get('data',pb) if isinstance(pb,dict) else {}
                            x=pick(pos,'xPercent'); y=pick(pos,'yPercent')
                            if x is not None and y is not None:
                                xy=(float(x),float(y)); location_ap_positions[str(serial)]=xy
                                amac=pick(a,'macAddress','mac','bssid')
                                if amac: location_ap_positions[norm_mac(amac)]=xy
                    except Exception: pass
        except Exception: pass
    c=load_config().get('location_engine',{})
    out=[]
    # Near-AP markers are only for clients RUCKUS One lists (the cards in the UI), never for every
    # device that happens to be heard. Everything else needs a real 3-AP fix.
    allowed={norm_mac(m) for m in macs.split(',') if m.strip()}
    def qualifies(k,v):
        if k in allowed: return True
        tail=v[-400:]
        if len({o['ap_id'] for o in tail})>=2: return True          # 3+ APs -> fix, 2 -> between
        return strangers and len(v)>=STRANGER_MIN_READINGS and max(o['rssi'] for o in tail)>=STRANGER_MIN_RSSI
    with _obs_lock:
        snapshot=[(k,list(v)) for k,v in location_observations.items() if qualifies(k,v)]
    now=time.time()
    live={k for k,_ in snapshot}
    for k in [k for k in location_stable if k not in live]: location_stable.pop(k,None); location_near_ap.pop(k,None)
    for cid,obs in snapshot:
        est=locate_client(cid,obs,now,c,cid in allowed,strangers)
        if est:
            # Approximate markers are remembered (as last known) only for listed clients; for everything else
            # they are live-only so the stored set cannot grow with every passing device.
            if est.get('kind') not in ('near','between') or cid in allowed: location_estimates[cid]=est
            out.append({'clientId':cid,'location':est,'stale':False,'ageSeconds':0})
    strangers_out=[o for o in out if o['location'].get('stranger')]
    if len(strangers_out)>STRANGER_MAX:
        keep={id(o) for o in sorted(strangers_out,key=lambda o:-o['location']['observations'][0]['rssi'])[:STRANGER_MAX]}
        out=[o for o in out if not o['location'].get('stranger') or id(o) in keep]
    # Clients not located right now (not heard by 3 APs within the last 30 s):
    # keep reporting their last known position, flagged stale, for up to 24 h.
    fresh={o['clientId'] for o in out}
    if allowed:
        for cid in [k for k,v in location_estimates.items() if v.get('kind') in ('near','between') and k not in allowed]:
            del location_estimates[cid]; location_near_ap.pop(cid,None)
    for cid,est in list(location_estimates.items()):
        age=now-float(est.get('timestamp') or 0)
        if age>86400: del location_estimates[cid]; continue
        if cid not in fresh and not (est.get('kind') in ('near','between') and cid not in allowed):
            out.append({'clientId':cid,'location':est,'stale':True,'ageSeconds':round(age)})
    _save_last_locations(now)
    aps_n,r1_n=lbs_receiver.client_counts() if lbs_receiver else (0,0)
    # Per-AP RSSI for the requested clients (the cards in the UI), strongest first.
    ap_rssi={}
    for m in [x for x in macs.split(',') if x.strip()][:300]:
        seen=location_ap_seen.get(norm_mac(m))
        if not seen: continue
        rows=[{'apId':ap,'rssi':round(v[0]),'ageSeconds':max(0,round(now-v[1]))} for ap,v in seen.items() if now-v[1]<=86400]
        if rows: ap_rssi[m]=sorted(rows,key=lambda r:-r['rssi'])
    for k in [k for k,v in location_ap_seen.items() if all(now-t>86400 for _,t in v.values())]: del location_ap_seen[k]
    return {'ok':True,'enabled':bool(c.get('enabled',True)),'locations':out,'apRssi':ap_rssi,'meta':{'lastTelemetry':location_meta['lastTelemetry'],'observationCount':location_meta['observationCount'],'receiverRunning':bool(lbs_receiver and lbs_receiver.running), 'receiverConnections':int(getattr(lbs_receiver,'connected_clients',0) if lbs_receiver else 0), 'receiverConnectionCount':int(getattr(lbs_receiver,'connection_count',0) if lbs_receiver else 0), 'receiverStage':getattr(lbs_receiver,'last_stage','stopped') if lbs_receiver else 'stopped', 'receiverConnectResult':getattr(lbs_receiver,'last_connect_result',None) if lbs_receiver else None, 'receiverMessages':int(getattr(lbs_receiver,'message_count',0) if lbs_receiver else 0),'receiverTlsHandshakeFailures':int(getattr(lbs_receiver,'tls_handshake_failures',0) if lbs_receiver else 0),'receiverLastPeer':getattr(lbs_receiver,'last_peer',None) if lbs_receiver else None,'receiverLastEvent':getattr(lbs_receiver,'last_event',None) if lbs_receiver else None, 'receiverLastMessage':getattr(lbs_receiver,'last_message',None) if lbs_receiver else None,'receiverError':location_meta['receiverError'],'apReferenceCount':len(location_ap_positions),'receiverAps':aps_n,'receiverRuckusOne':r1_n,'apHealth':lbs_receiver.ap_health() if lbs_receiver else []}}

@app.get('/api/config/location-engine')
def location_config():
    c=load_config().get('location_engine',{})
    return {'ok':True,'location_engine':{'enabled':bool(c.get('enabled',True)),'reference_rssi':float(c.get('reference_rssi',-45)),'path_loss_exponent':float(c.get('path_loss_exponent',2.2)),'smoothing':float(c.get('smoothing',0.35)),'live_seconds':live_window(c)}}

@app.post('/api/config/location-engine')
def configure_location_engine(payload:dict):
    c=load_config(); old=c.get('location_engine',{}) if isinstance(c.get('location_engine',{}),dict) else {}
    c['location_engine']={'enabled':bool(payload.get('enabled',old.get('enabled',True))),'reference_rssi':float(payload.get('reference_rssi',old.get('reference_rssi',-45))),'path_loss_exponent':float(payload.get('path_loss_exponent',old.get('path_loss_exponent',2.2))),'smoothing':float(payload.get('smoothing',old.get('smoothing',0.35))),'live_seconds':live_window({'live_seconds':payload.get('live_seconds',old.get('live_seconds',LIVE_SECONDS))})}
    save_config(c); return {'ok':True,'live_seconds':c['location_engine']['live_seconds']}

@app.get('/api/r1/discovery')
async def discovery(venue_id:str='',floorplan_id:str=''):
    if not require_r1(): return {'ok':False,'error':'RUCKUS One is not configured.'}
    v=await venues();
    if not v.get('ok'): return v
    vs=v.get('data',[]); chosen=next((x for x in vs if str(pick(x,'id'))==venue_id),None) if venue_id else (vs[0] if vs else None)
    if not chosen:return {'ok':True,'venues':vs,'venue':None,'floorplans':[],'aps':[],'clients':[]}
    vid=str(pick(chosen,'id')); fp=await floorplans(vid); fps=fp.get('data',[]) if fp.get('ok') else []
    chosenfp=next((x for x in fps if str(pick(x,'id'))==floorplan_id),None) if floorplan_id else (fps[0] if fps else None)
    aid=await aps(vid); cli=await r1clients(vid); apdata=aid.get('data',[]) if aid.get('ok') else []; clidata=cli.get('data',[]) if cli.get('ok') else []
    # Normalize APs and use the documented floor-position endpoint for the selected floorplan.
    location_ap_positions.clear()
    async def enrich_ap(a):
        pos=pick(a,'floorPosition',default={}) or {}
        serial=pick(a,'serialNumber')
        if chosenfp and serial:
            try:
                pr=await r.get(f'/venues/{vid}/floorplans/{pick(chosenfp,"id")}/aps/{serial}/floorPositions')
                if pr.is_success:
                    pb=pr.json()
                    if isinstance(pb,dict):
                        candidate=pb.get('data',pb)
                        if isinstance(candidate,dict): pos=candidate
            except Exception:
                pass
        amac=pick(a,'macAddress','mac','bssid')
        if pos.get('xPercent') is not None and pos.get('yPercent') is not None:
            xy=(float(pos.get('xPercent')),float(pos.get('yPercent')))
            if serial: location_ap_positions[str(serial)]=xy
            if amac: location_ap_positions[norm_mac(amac)]=xy
        return {'serialNumber':serial,'name':pick(a,'name'),'model':pick(a,'model'),'macAddress':amac,'venueId':pick(a,'venueId'),'status':pick(a,'status'),'healthStatus':pick(a,'healthStatus'),'clientCount':pick(a,'clientCount',default=0),'lbsStatus':pick(a,'lbsStatus',default={}) or {},'radioStatuses':pick(a,'radioStatuses',default=[]) or [],'floorPosition':{'xPercent':pick(pos,'xPercent'),'yPercent':pick(pos,'yPercent')}}
    normalized=await asyncio.gather(*(enrich_ap(a) for a in apdata))
    return {'ok':True,'venues':vs,'venue':chosen,'floorplans':fps,'floorplan':chosenfp,'floorplanImageId':pick(chosenfp,'imageId') if chosenfp else None,'floorplanImageName':pick(chosenfp,'imageName') if chosenfp else None,'aps':normalized,'clients':clidata}

@app.get('/api/site')
def site(): return {'tenant':'Demo Tenant','venue':'Building A','floor':{'id':2,'name':'Floor 2'},'aps':APs,'clients':sim_snapshot(),'mode':'simulation'}
@app.websocket('/ws')
async def ws(sock:WebSocket):
    await sock.accept(); connections.add(sock)
    try:
        while True:
            simulate(); await sock.send_text(json.dumps({'type':'positions','clients':sim_snapshot()})); await asyncio.sleep(2)
    except Exception: pass
    finally: connections.discard(sock)

HTML='''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>RUCKUS Location Services v1.7</title><style>
:root{--bg:#091016;--panel:#111a22;--card:#17232d;--text:#edf3f6;--muted:#91a0aa;--border:#2b3a46;--accent:#6dc1df;--good:#62ca91;--warn:#e4b35e;--bad:#e47777}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:14px Segoe UI,Arial,sans-serif}main{max-width:1400px;margin:auto;padding:24px}.top{display:flex;justify-content:space-between;align-items:end;gap:20px;margin-bottom:14px}.eyebrow{font-size:11px;letter-spacing:.12em;text-transform:uppercase;color:var(--muted);font-weight:700}h1{margin:5px 0;font-size:28px}.toolbar{display:flex;gap:8px;align-items:center}.btn,select,input{border:1px solid var(--border);background:var(--card);color:var(--text);border-radius:8px;padding:9px 11px}.btn{cursor:pointer}.btn.primary{border-color:#42778d;background:#16303b}.status{font-size:12px;color:var(--good)}.bar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;background:var(--panel);padding:10px;border:1px solid var(--border);border-radius:12px;margin-bottom:14px}.bar select{min-width:190px}.grid{display:grid;grid-template-columns:minmax(0,1fr) 340px;gap:14px}.panel{background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:14px}.map{position:relative;aspect-ratio:16/9;background:#fff;border:1px solid var(--border);border-radius:10px;overflow:hidden}.floorImage{position:absolute;inset:0;width:100%;height:100%;object-fit:contain;z-index:1;background:#fff}.floor{position:absolute;inset:7%;z-index:2;border:1px solid #56636d;background:transparent}.ap,.client{position:absolute;transform:translate(-50%,-50%);z-index:3}.ap{width:16px;height:16px;border:2px solid var(--accent);border-radius:50%;background:#132733}.ap:after{content:"";position:absolute;inset:-7px;border:1px solid #6dc1df33;border-radius:50%}.client{width:13px;height:13px;border-radius:50%;background:#e7b35d;box-shadow:0 0 0 4px #e7b35d22}.client.sel{background:#74d6ff;box-shadow:0 0 0 5px #74d6ff33}.label{position:absolute;top:17px;left:50%;transform:translateX(-50%);font-size:10px;white-space:nowrap;color:var(--muted)}.client span{position:absolute;top:15px;left:50%;transform:translateX(-50%);font-size:10px;white-space:nowrap}
.client.live{cursor:pointer;width:11px;height:11px}
.client.live span{display:none;top:16px;background:#111a22;color:#edf3f6;border:1px solid var(--border);border-radius:4px;padding:2px 6px;font-size:11px;pointer-events:none}
.client.live:hover,.client.live.sel{z-index:6}
.client.live:hover span,.client.live.sel span{display:block}
.client.live.sel{width:15px;height:15px;box-shadow:0 0 0 6px #74d6ff55}
.client.live.near{background:#fff0;border:2px dashed #e7b35d;box-shadow:none}
.client.live.between{border-radius:3px}
.client.live.stranger{width:8px;height:8px;border-width:1.5px;opacity:.8}
.client.live.near.stale{border-color:#8b98a3}
.client.live.near.sel{border-color:#74d6ff;box-shadow:0 0 0 6px #74d6ff55}
.client.live.stale{background:#8b98a3;box-shadow:0 0 0 4px #8b98a333}
.client.live.stale.sel{background:#8b98a3;box-shadow:0 0 0 6px #74d6ff77}
.client.live.near.stale.sel{background:#fff0}
.card.loc.stale{border-left-color:#8b98a3}
.apstrip{display:flex;gap:8px;align-items:center;flex-wrap:wrap;background:var(--panel);padding:8px 10px;border:1px solid var(--border);border-radius:12px;margin-bottom:14px;font-size:12px}
.apchip{display:inline-flex;gap:6px;align-items:center;border:1px solid var(--border);border-radius:999px;padding:3px 10px;background:var(--card);cursor:default}
.apchip .dot{width:9px;height:9px;border-radius:50%;background:var(--muted)}.apchip.ok .dot{background:var(--good)}.apchip.warn .dot{background:var(--warn)}.apchip.bad .dot{background:var(--bad)}
.apchip.bad{border-color:#e4777788}.apchip.warn{border-color:#e4b35e88}.apchip.other{opacity:.65}
.apsum{font-weight:700}.apsum.ok{color:var(--good)}.apsum.bad{color:var(--bad)}
.seg{display:inline-flex}.seg .btn{border-radius:0;margin:0}.seg .btn:first-child{border-radius:8px 0 0 8px}.seg .btn:last-child{border-radius:0 8px 8px 0;border-left:0}.seg .btn.active{background:#16303b;border-color:#42778d;color:var(--accent)}
.ap .label{top:21px;color:#0b1c26;background:#ffffffee;border:1px solid #4f9db8;border-radius:4px;padding:1px 6px;font-size:12px;font-weight:600;line-height:1.35;pointer-events:none;box-shadow:0 1px 3px #0003}
.ap:hover{z-index:7}
.card.loc{border-left:3px solid var(--good)}
.card.sel{outline:2px solid var(--accent);background:#1d3442}.cards{display:grid;gap:8px;max-height:620px;overflow:auto}.card{border:1px solid var(--border);background:var(--card);padding:10px;border-radius:9px;cursor:pointer}.name{font-weight:700}.kv{display:flex;justify-content:space-between;gap:10px;font-size:12px;margin-top:5px}.muted{color:var(--muted)}.pill{padding:3px 7px;border-radius:20px;background:#1c3039;color:var(--accent);font-size:11px}.modal{display:none;position:fixed;inset:0;background:#000b;align-items:center;justify-content:center;padding:20px;z-index:10}.modal.open{display:flex}.dialog{width:min(620px,100%);background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:20px}.row{display:grid;gap:5px;margin:10px 0}label{font-size:12px;color:var(--muted);font-weight:600}.result{margin-top:12px;padding:10px;border-radius:8px;background:var(--card);white-space:pre-wrap}.good{border:1px solid #35694b}.bad{border:1px solid #713f3f}@media(max-width:900px){.grid{grid-template-columns:1fr}.top{align-items:start;flex-direction:column}}
</style></head><body><main><div class="top"><div><div class="eyebrow">RUCKUS One Location Services</div><h1>Live Client Location <span class="pill">v1.7</span></h1></div><div class="toolbar"><span id="status" class="status">RUCKUS One</span><button class="btn primary" onclick="openSettings()">RUCKUS One Setup</button><button class="btn" onclick="openLBS()">LBS Receiver</button></div></div>
<div class="bar"><b>Data source</b><select id="mode" onchange="changeMode()"><option value="r1">RUCKUS One</option><option value="sim">Simulator</option></select><select id="venue" onchange="venueChanged()" disabled><option>Venue</option></select><select id="floor" onchange="floorChanged()" disabled><option>Floor</option></select><button class="btn" id="refresh" onclick="loadR1()" disabled>Refresh</button><span class="seg" title="Selected client: the map shows only the client you click and the list shows everyone. All located clients: map and list show every client that has a position (real fixes, between-AP and near-AP markers, and last known). Live fixes (3+ APs): only clients heard by 3 or more APs right now, with a real triangulated position."><button class="btn active" id="viewSel" onclick="setView(false)">Selected client</button><button class="btn" id="viewAll" onclick="setView(true)">All located clients</button><button class="btn" id="viewFix" onclick="setView('fix')">Live fixes (3+ APs)</button></span><label class="muted" style="display:inline-flex;gap:5px;align-items:center;cursor:pointer" title="Also show unlisted devices that are strong (-70 dBm or better) and steady near an AP, as near-AP markers. They appear in All located clients view only and are not remembered when they go quiet."><input type="checkbox" id="strangersChk" onchange="strangersToggled()">Nearby unlisted devices</label><label class="muted" style="display:inline-flex;gap:5px;align-items:center" title="A client counts as located (live) only if at least 3 APs heard it within this many seconds; otherwise it falls back to a between-APs / near-AP marker or its last known position. Longer = more clients get a real fix, but positions are older and less accurate. 5 to 300 seconds."><span>3 APs within</span><input type="number" id="liveSecs" min="5" max="300" step="5" value="30" style="width:64px;padding:6px 8px" onchange="saveLiveSecs()"><span>s</span></label><span id="summary" class="muted">5 simulated clients</span><span id="lbsSummary" class="pill">LBS: not checked</span><span id="receiverSummary" class="pill">Receiver: stopped</span><span id="locSummary" class="pill">Location: waiting</span></div>
<div class="apstrip" id="apStrip"><span class="muted">Access points: waiting for data</span></div>
<div class="grid"><section class="panel"><div style="display:flex;justify-content:space-between;margin-bottom:9px"><b id="mapTitle">Demo Tenant · Building A · Floor 2</b><span class="muted" id="updated">Live</span></div><div class="map" id="map"><img id="floorImage" class="floorImage" alt="RUCKUS One floor plan" style="display:none"><div class="floor" id="floorBg" style="display:none"></div></div><div class="muted" style="font-size:12px;margin-top:9px">AP markers use RUCKUS One floor-plan coordinates when available. The floor-plan is displayed on a clean white background; the image is retrieved from the RUCKUS One signed-image service. Client markers appear when usable LBS RSSI observations are received; AP-anchor and RSSI estimates are explicitly labeled.</div></section><aside class="panel"><div class="eyebrow">Selected Client</div><div id="detail" style="margin:8px 0 14px"><span class="muted">Select a client.</span></div><div class="eyebrow">Selected AP</div><div id="apDetail" style="margin:8px 0 14px"><span class="muted">Click an AP marker.</span></div><div class="eyebrow">Clients</div><div id="clients" class="cards" style="margin-top:8px"></div></aside></div></main>
<div class="modal" id="modal"><div class="dialog"><div style="display:flex;justify-content:space-between;align-items:center"><div><div class="eyebrow">Integration</div><h2 style="margin:4px 0">RUCKUS One Connection</h2></div><button class="btn" onclick="closeSettings()">Close</button></div><p class="muted" style="font-size:13px">Tenant and Client ID are stored in the persistent local configuration. The Shared Secret is stored separately by the application. Never paste your Shared Secret into chat.</p><div class="row"><label>Authentication URL</label><input id="auth" value="https://ruckus.cloud"></div><div class="row"><label>REST API Base URL</label><input id="base" value="https://api.ruckus.cloud"></div><div class="row"><label>Tenant ID</label><input id="tenant"></div><div class="row"><label>Client ID</label><input id="client"></div><div class="row"><label>Shared Secret</label><input id="secret" type="password"></div><div style="display:flex;gap:8px;justify-content:flex-end"><button class="btn" onclick="recoverConfig()">Recover Previous Config</button><button class="btn primary" onclick="saveAndTest()">Save & Test</button></div><div id="result" class="result" style="display:none"></div></div></div>
<script>
let mode='r1', selected=null, simClients=[];let ws;
function $(x){return document.getElementById(x)}
function openSettings(){ $('modal').classList.add('open'); fetch('/api/config').then(r=>r.json()).then(c=>{let x=c.ruckus_one||{};$('auth').value=x.auth_url||'https://ruckus.cloud';$('base').value=x.base_url||'https://api.ruckus.cloud';$('tenant').value=x.tenant_id||'';$('client').value=x.client_id||''})}
function closeSettings(){$('modal').classList.remove('open')}
async function recoverConfig(){try{let r=await fetch('/api/config/recover',{method:'POST'});let j=await r.json();if(j.ok){$('tenant').value=j.tenant_id||'';$('client').value=j.client_id||'';$('secret').value='';$('secret').placeholder='Saved — leave blank to keep it';show('Recovered previous configuration. Click Save & Test.',false)}else show(j.error,true)}catch(e){show('Recovery error: '+e,true)}}
async function saveAndTest(){let p={auth_url:$('auth').value,base_url:$('base').value,tenant_id:$('tenant').value,client_id:$('client').value,client_secret:$('secret').value};let s=await fetch('/api/config/ruckus-one',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});let a=await s.json();if(!a.ok)return show(a.error,true);let t=await fetch('/api/config/test',{method:'POST'});let b=await t.json();show(b.ok?b.message:b.error,!b.ok);if(b.ok){$('status').textContent='RUCKUS One authenticated';$('status').style.color='var(--good)';closeSettings();$('mode').value='r1';changeMode()}}
function show(t,bad){$('result').style.display='block';$('result').className='result '+(bad?'bad':'good');$('result').textContent=t}
function changeMode(){mode=$('mode').value;if(mode==='sim'){enable(false);$('status').textContent='Simulation';$('summary').textContent='5 simulated clients';connectWS()}else{enable(true);if(ws){try{ws.close()}catch(e){}};loadR1()}}
function enable(v){$('venue').disabled=!v;$('floor').disabled=!v;$('refresh').disabled=!v}
function renderAPs(aps){window.r1APs=aps;document.querySelectorAll('.ap').forEach(e=>e.remove());for(let a of aps){let p=a.floorPosition||{};if(p.xPercent==null||p.yPercent==null)continue;let d=document.createElement('div');d.className='ap';d.style.left=p.xPercent+'%';d.style.top=p.yPercent+'%';d.innerHTML='<span class="label">'+esc(a.name||a.serialNumber||'AP')+'</span>';d.title=(a.name||a.serialNumber)+' · '+(a.model||'');d.onclick=()=>selectAP(a);$('map').appendChild(d)}}
function selectAP(a){
 selectedAP=a;
 let l=a.lbsStatus||{}, radios=a.radioStatuses||[];
 let html='<div class="name">'+esc(a.name||a.serialNumber||'Access Point')+'</div>'
 +'<div class="kv"><span class="muted">Serial</span><span>'+esc(a.serialNumber||'—')+'</span></div>'
 +'<div class="kv"><span class="muted">Model</span><span>'+esc(a.model||'—')+'</span></div>'
 +'<div class="kv"><span class="muted">MAC</span><span>'+esc(a.macAddress||'—')+'</span></div>'
 +'<div class="kv"><span class="muted">Status</span><span>'+esc(a.status||'—')+'</span></div>'
 +'<div class="kv"><span class="muted">Health</span><span>'+esc(a.healthStatus||'—')+'</span></div>'
 +'<div class="kv"><span class="muted">Clients</span><span>'+esc(a.clientCount??0)+'</span></div>'
 +'<div class="kv"><span class="muted">LBS server</span><span>'+esc(l.serverConnected===true?'Connected':l.serverConnected===false?'Disconnected':'—')+'</span></div>'
 +'<div class="kv"><span class="muted">LBS management</span><span>'+esc(l.managementConnected===true?'Connected':l.managementConnected===false?'Disconnected':'—')+'</span></div>'
 +'<div style="margin-top:8px"><span class="muted">Radios: </span>'+esc(radios.length)+'</div>';
 if(radios.length){html+='<div style="margin-top:7px">'+radios.map((r,i)=>'<div class="kv"><span class="muted">Radio '+(i+1)+'</span><span>'+esc(r.band||r.radioBand||r.channel||'—')+'</span></div>').join('')+'</div>'}
 $('apDetail').innerHTML=html;
 // Highlight associated clients in the list.
 let key=a.serialNumber||a.macAddress||a.name||'';
 document.querySelectorAll('#clients .card').forEach(card=>card.style.outline='');
 for(let c of (window.r1Clients||[])){let ca=c.apInformation||{};if(String(ca.serialNumber||ca.name||ca.macAddress||'')===String(key)){let card=[...document.querySelectorAll('#clients .card')].find(x=>x.dataset.mac===String(c.macAddress||''));if(card)card.style.outline='1px solid var(--accent)'}}
}

function renderClients(cs){window.r1Clients=cs;$('clients').innerHTML='';for(let c of cs){let mac=c.macAddress||c.mac||'';let card=document.createElement('div');card.className='card';card.dataset.mac=mac;card.onclick=()=>selectClient(c);card.innerHTML='<div class="name">'+esc(c.hostname||c.alias||c.name||mac||'Client')+'</div><div class="kv"><span class="muted">MAC</span><span>'+esc(mac)+'</span></div><div class="kv"><span class="muted">AP</span><span>'+esc((c.apInformation||{}).name||'—')+'</span></div><div class="kv"><span class="muted">RSSI / SNR</span><span>'+esc(((c.signalStatus||{}).rssi??'—')+' / '+((c.signalStatus||{}).snr??'—'))+'</span></div>'; $('clients').appendChild(card)}}
function selectClient(c){selected=c;let s=c.signalStatus||{},a=c.apInformation||{},n=c.networkInformation||{};$('detail').innerHTML='<div class="name">'+esc(c.hostname||c.alias||c.macAddress||'Client')+'</div><div class="kv"><span class="muted">MAC</span><span>'+esc(c.macAddress||'—')+'</span></div><div class="kv"><span class="muted">AP</span><span>'+esc(a.name||a.serialNumber||'—')+'</span></div><div class="kv"><span class="muted">SSID</span><span>'+esc(n.ssid||'—')+'</span></div><div class="kv"><span class="muted">RSSI</span><span>'+esc(s.rssi??'—')+' dBm</span></div><div class="kv"><span class="muted">SNR</span><span>'+esc(s.snr??'—')+' dB</span></div><div class="kv"><span class="muted">Noise</span><span>'+esc(s.noiseFloor??'—')+' dBm</span></div><div class="kv"><span class="muted">Band</span><span>'+esc(c.band||'—')+'</span></div><div class="kv"><span class="muted">Channel</span><span>'+esc((c.radioStatus||{}).channel??'—')+'</span></div>';if(mode==='r1')renderLiveLocations(lastLocs)}
async function populateVenues(preserveId=''){let v=await fetch('/api/r1/venues').then(r=>r.json());if(!v.ok){$('summary').textContent='API error: '+(v.error||'Unknown error');return null}let vs=v.data||[];let wanted=preserveId||$('venue').value;let chosen=vs.find(x=>String(x.id)===String(wanted))||vs[0];$('venue').innerHTML=vs.map(x=>'<option value="'+esc(x.id)+'">'+esc(x.name||x.id)+'</option>').join('');if(chosen)$('venue').value=String(chosen.id);return {venues:vs,venueId:chosen?String(chosen.id):''}}
async function loadFloorImage(venueId,floorplan){let img=$('floorImage');img.style.display='none';let old=img.dataset.objectUrl;if(old){try{URL.revokeObjectURL(old)}catch(e){}delete img.dataset.objectUrl}img.removeAttribute('src');if(!floorplan||!floorplan.imageId)return;let u='/api/r1/floorplan-image?venue_id='+encodeURIComponent(venueId)+'&image_id='+encodeURIComponent(floorplan.imageId);try{let r=await fetch(u,{cache:'no-store'});if(!r.ok){console.warn('Floor plan image HTTP error',r.status);return}let b=await r.blob();let url=URL.createObjectURL(b);img.dataset.objectUrl=url;img.onload=()=>{img.style.display='block'};img.onerror=()=>{img.style.display='none';console.warn('Floor plan image could not be rendered',b.type,b.size)};img.src=url}catch(e){console.warn('Floor plan image load failed',e)}}
function lsGet(k){try{return localStorage.getItem(k)||''}catch(e){return ''}}
function lsSet(k,v){try{localStorage.setItem(k,v)}catch(e){}}
let selRestored=false;
function rememberSel(){let v=$('venue').value,f=$('floor').value;if(v&&f&&v!=='Venue'&&f!=='Floor'){lsSet('lastVenue',v);lsSet('lastFloor',f)}}
async function loadR1(options={}){let currentVenue=options.venueId||$('venue').value;let currentFloor=options.floorId||$('floor').value;
 if(!selRestored){selRestored=true;if(!options.venueId){let sv=lsGet('lastVenue');if(sv){currentVenue=sv;currentFloor=lsGet('lastFloor')}}}
let vr=await populateVenues(currentVenue);if(!vr||!vr.venueId){$('summary').textContent='No venues returned';return}let vid=vr.venueId;let d=await fetch('/api/r1/discovery?venue_id='+encodeURIComponent(vid)).then(r=>r.json());if(!d.ok){$('summary').textContent='Discovery error: '+(d.error||'Unknown error');return}let fps=d.floorplans||[];let wantedFloor=currentFloor;let chosenFloor=fps.find(x=>String(x.id)===String(wantedFloor))||fps[0];$('floor').innerHTML=fps.map(x=>'<option value="'+esc(x.id)+'">Floor '+esc(x.floorNumber??'')+' · '+esc(x.name||x.id)+'</option>').join('');if(chosenFloor)$('floor').value=String(chosenFloor.id);let fid=chosenFloor?String(chosenFloor.id):'';let d2=await fetch('/api/r1/discovery?venue_id='+encodeURIComponent(vid)+'&floorplan_id='+encodeURIComponent(fid)).then(r=>r.json());if(!d2.ok){$('summary').textContent='Discovery error: '+(d2.error||'Unknown error');return}let venue=d2.venue||{};$('mapTitle').textContent=(venue.name||'Venue')+' · '+((d2.floorplan||{}).name||'Floorplan');loadFloorImage(vid,d2.floorplan);$('summary').textContent=(d2.aps||[]).length+' APs · '+(d2.clients||[]).length+' clients';checkLBS(vid, venue.name||'');$('updated').textContent='Updated '+new Date().toLocaleTimeString();renderAPs(d2.aps||[]);renderClients(d2.clients||[]);rememberSel()}
async function venueChanged(){let vid=$('venue').value;$('floor').innerHTML='<option>Loading floors…</option>';await loadR1({venueId:vid,floorId:''})}
async function floorChanged(){let vid=$('venue').value,fid=$('floor').value;if(!vid)return;let d=await fetch('/api/r1/discovery?venue_id='+encodeURIComponent(vid)+'&floorplan_id='+encodeURIComponent(fid)).then(r=>r.json());if(!d.ok){$('summary').textContent='Discovery error: '+(d.error||'Unknown error');return}let venue=d.venue||{};$('mapTitle').textContent=(venue.name||'Venue')+' · '+((d.floorplan||{}).name||'Floorplan');loadFloorImage(vid,d.floorplan);$('summary').textContent=(d.aps||[]).length+' APs · '+(d.clients||[]).length+' clients';checkLBS(vid, venue.name||'');$('updated').textContent='Updated '+new Date().toLocaleTimeString();renderAPs(d.aps||[]);renderClients(d.clients||[]);rememberSel()}
async function checkLBS(vid,vname=''){try{let r=await fetch('/api/r1/lbs-profiles?venue_id='+encodeURIComponent(vid)+'&venue_name='+encodeURIComponent(vname));let j=await r.json();if(!j.ok){$('lbsSummary').textContent='LBS: API error';return}let xs=j.data||[];let connected=j.connected??xs.filter(x=>x.serverConnected===true).length;if((window.rxR1||0)>0)connected=Math.max(connected,1);let total=j.totalProfiles??xs.length;let mgmt=j.managementConnected??0;let profileCount=j.venueProfiles??xs.length;if(profileCount&&connected===0&&window.rxRunning){$('lbsSummary').textContent='LBS: waiting for RUCKUS One to reconnect';$('lbsSummary').title='Your receiver is listening, but RUCKUS One has not reconnected to it yet. This is normal after restarting this app or removing and re-adding the LBS profile on a venue; RUCKUS One reconnects on its own and can take several minutes. APs that are already reporting keep working meanwhile. · '+profileCount+' profile(s) for this venue';}else if(profileCount){$('lbsSummary').textContent=profileCount+' LBS profile'+(profileCount===1?'':'s')+' · '+connected+' connected';$('lbsSummary').title='Selected venue: '+profileCount+' profile(s) · '+connected+' server connected · '+mgmt+' management connected · '+total+' total tenant profile(s)';}else if(total){$('lbsSummary').textContent='LBS: '+total+' profile'+(total===1?'':'s')+' · none for venue';$('lbsSummary').title='RUCKUS One returned '+total+' LBS profile(s), but none are associated with the selected venue.';}else{$('lbsSummary').textContent='LBS: no profile';$('lbsSummary').title='No LBS server profiles were returned by RUCKUS One.';}}catch(e){$('lbsSummary').textContent='LBS: unavailable';$('lbsSummary').title=e.message||'LBS lookup failed'}}
function connectWS(){if(ws)try{ws.close()}catch(e){};ws=new WebSocket((location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws');ws.onmessage=e=>{let d=JSON.parse(e.data);if(d.type==='positions'){simClients=d.clients;renderSim(d.clients);$('updated').textContent='Live '+new Date().toLocaleTimeString()}}}
function renderSim(cs){document.querySelectorAll('.client').forEach(e=>e.remove());$('clients').innerHTML='';for(let c of cs){let d=document.createElement('div');d.className='client'+(selected&&selected.mac===c.mac?' sel':'');d.style.left=c.x+'%';d.style.top=c.y+'%';d.innerHTML='<span>'+esc(c.name)+'</span>';d.onclick=()=>{selected=c;renderSim(cs);selectClient({macAddress:c.mac,hostname:c.name,apInformation:{name:c.ap},networkInformation:{ssid:c.wlan},signalStatus:{rssi:c.rssi}})};$('map').appendChild(d);let card=document.createElement('div');card.className='card';card.onclick=()=>{selected=c;renderSim(cs);selectClient({macAddress:c.mac,hostname:c.name,apInformation:{name:c.ap},networkInformation:{ssid:c.wlan},signalStatus:{rssi:c.rssi}})};card.innerHTML='<div class="name">'+esc(c.name)+'</div><div class="kv"><span class="muted">AP</span><span>'+esc(c.ap)+'</span></div><div class="kv"><span class="muted">RSSI</span><span>'+esc(c.rssi)+' dBm</span></div>'; $('clients').appendChild(card)}}
function esc(v){return String(v??'').replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}
async function openLBS(){let r=await fetch('/api/config/lbs');let j=await r.json();let c=j.lbs_server||{};$('lbsBind').value=c.bind_address||'0.0.0.0';$('lbsAdvertised').value=c.advertised_ip||'';$('lbsPort').value=c.port||8883;$('lbsVenueName').value=c.lbs_server_venue_name||'';$('lbsPass').value='';$('lbsPass').placeholder=c.password_set?'Saved — leave blank to keep it':'';$('lbsTopic').value=c.topic||'#';$('lbsTls').checked=true;$('lbsInstall').textContent=c.mosquitto_found?'Mosquitto broker: detected':'Mosquitto broker: NOT detected';$('lbsInstall').className='result '+(c.mosquitto_found?'good':'bad');$('lbsModal').classList.add('open')}
function closeLBS(){$('lbsModal').classList.remove('open')}
async function restartLBS(){let p={bind_address:$('lbsBind').value,advertised_ip:$('lbsAdvertised').value,port:$('lbsPort').value,lbs_server_venue_name:$('lbsVenueName').value,password:$('lbsPass').value,topic:$('lbsTopic').value};let r=await fetch('/api/config/lbs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(p)});let j=await r.json();$('lbsResult').style.display='block';$('lbsResult').className='result '+(j.ok?'good':'bad');$('lbsResult').textContent=j.ok?j.message:(j.error||'Unable to start LBS receiver');if(j.ok){$('lbsDiag').textContent='Receiver started. The toolbar will show the current connection stage. Keep this window open while RUCKUS One connects. Use the broker log and /api/lbs/diagnostics to inspect the MQTT exchange.';closeLBS();updateLocationStatus()}else{$('lbsDiag').textContent='Receiver failed to start: '+(j.error||'unknown error')}}
let locBusy=false;
async function updateLocationStatus(){if(locBusy)return;locBusy=true;try{let cardMacs=[...document.querySelectorAll('#clients .card')].map(c=>c.dataset.mac).filter(Boolean).join(',');let r=await fetch('/api/r1/location?venue_id='+encodeURIComponent($('venue').value)+'&floorplan_id='+encodeURIComponent($('floor').value)+'&macs='+encodeURIComponent(cardMacs)+'&strangers='+($('strangersChk').checked?1:0));let j=await r.json();let locs=j.locations||[];let m=j.meta||{};let kd=x=>(x.location||{}).kind,nFix=locs.filter(x=>!x.stale&&kd(x)!=='near'&&kd(x)!=='between').length,nBtw=locs.filter(x=>!x.stale&&kd(x)==='between').length,nNear=locs.filter(x=>!x.stale&&kd(x)==='near'&&!(x.location||{}).stranger).length,nStr=locs.filter(x=>!x.stale&&(x.location||{}).stranger).length,nOld=locs.filter(x=>x.stale).length;$('locSummary').textContent=nFix+' located'+(nBtw?' · '+nBtw+' between APs':'')+(nNear?' · '+nNear+' near AP':'')+(nStr?' · '+nStr+' nearby':'')+(nOld?' · '+nOld+' last known':'');$('locSummary').title='LBS observations: '+(m.observationCount||0)+' · AP references: '+(m.apReferenceCount||0);window.rxRunning=!!m.receiverRunning;window.rxR1=m.receiverRuckusOne||0;if(window.rxR1prev!==window.rxR1){window.rxR1prev=window.rxR1;let vo=$('venue').options[$('venue').selectedIndex];checkLBS($('venue').value,vo?vo.textContent:'')}let stage=m.receiverStage||'stopped';let labels={listening_tls_psk:'Listening (TLS-PSK)',broker_starting:'Starting Mosquitto',location_app_connected:'Broker connected',requests_published:'LBS requests published',listening_tls:'Listening (TLS)',listening_plaintext:'Listening',tcp_connected_tls_ok:'TCP/TLS connected',mqtt_connect_received:'MQTT CONNECT received',mqtt_authenticated:'MQTT authenticated',mqtt_authenticated_waiting_subscribe:'MQTT authenticated · waiting for SUBSCRIBE',mqtt_subscribed:'MQTT SUBSCRIBE received',mqtt_auth_failed:'MQTT password rejected',mqtt_session_active:'MQTT session active',tls_handshake_failed:'TLS handshake failed',tls_no_shared_cipher:'TLS PSK cipher mismatch',publish_received:'LBS message received',connection_closed:'Connection closed',receiver_error:'Receiver error',listener_error:'Listener error',stopped:'Stopped'};$('receiverSummary').textContent='Receiver: '+(labels[stage]||stage);$('receiverSummary').title='Stage: '+stage+' · connections: '+(m.receiverConnectionCount||0)+' · APs connected: '+(m.receiverAps||0)+' · RUCKUS One connected: '+(m.receiverRuckusOne?'yes':'no')+' · messages: '+(m.receiverMessages||0)+(m.receiverTlsHandshakeFailures?' · TLS failures: '+m.receiverTlsHandshakeFailures:'')+(m.receiverLastPeer?' · peer: '+m.receiverLastPeer:'')+(m.receiverConnectResult?' · auth: '+m.receiverConnectResult:'')+(m.receiverError?' · error: '+m.receiverError:'');renderLiveLocations(locs);applyApRssi(j.apRssi);renderApStrip(j.meta&&j.meta.apHealth);if(m.lastTelemetry)$('updated').textContent='Telemetry '+new Date(m.lastTelemetry*1000).toLocaleTimeString()}catch(e){$('locSummary').textContent='Location: unavailable'}finally{locBusy=false}}
let showAll=false,lastLocs=[],scrollCard=false,locOnly=false,fixOnly=false;
function macKey(v){return String(v||'').toLowerCase().replace(/[^0-9a-f]/g,'')}
function findClient(mac){let k=macKey(mac);return (window.r1Clients||[]).find(x=>macKey(x.macAddress||x.mac)===k)}
function clientName(mac){let c=findClient(mac);return c?(c.hostname||c.alias||c.name||''):''}
function fmtAge(s){s=Math.round(s||0);if(s<90)return s+'s';if(s<5400)return Math.round(s/60)+' min';return (s/3600).toFixed(1)+' h'}
function apName(mac){let k=macKey(mac);let a=(window.r1APs||[]).find(x=>macKey(x.macAddress)===k);if(a)return a.name||a.serialNumber;let p=(window.r1APs||[]).find(x=>x.macAddress&&macKey(x.macAddress).slice(0,10)===k.slice(0,10));return p?(p.name||p.serialNumber):mac}
let lastApRssi={};
function apRowsHtml(rows){
 if(!rows||!rows.length)return '<div class="muted" style="font-size:11px;margin-top:6px">No AP readings yet</div>';
 return '<div class="muted" style="font-size:11px;margin-top:6px">RSSI by AP</div>'+rows.map(r=>{let pct=Math.max(0,Math.min(100,(r.rssi+100)*100/60));let old=r.ageSeconds>30;return '<div class="kv" style="align-items:center;opacity:'+(old?0.55:1)+'"><span>'+esc(apName(r.apId))+'</span><span style="display:flex;gap:6px;align-items:center"><span style="display:inline-block;width:46px;height:5px;background:#2b3a46;border-radius:3px;overflow:hidden"><span style="display:block;height:100%;width:'+pct+'%;background:'+(r.rssi>=-67?'var(--good)':r.rssi>=-80?'var(--warn)':'var(--bad)')+'"></span></span><b>'+r.rssi+' dBm</b>'+(old?'<span class="muted" style="font-size:10px">'+fmtAge(r.ageSeconds)+'</span>':'')+'</span></div>'}).join('')}
function renderSelAp(){if(!selected||mode!=='r1')return;let old=$('selAp');if(old)old.remove();let d=document.createElement('div');d.id='selAp';d.innerHTML=apRowsHtml(lastApRssi[macKey(selected.macAddress||selected.mac)]);$('detail').appendChild(d)}
function applyApRssi(map){map=map||{};lastApRssi={};for(let m in map)lastApRssi[macKey(m)]=map[m];
 document.querySelectorAll('#clients .card').forEach(card=>{let box=card.querySelector('.apr');if(!box){box=document.createElement('div');box.className='apr';card.appendChild(box)}box.innerHTML=apRowsHtml(lastApRssi[macKey(card.dataset.mac)])});
 renderSelAp()}
async function saveLiveSecs(){let v=Math.max(5,Math.min(300,parseInt($('liveSecs').value)||30));$('liveSecs').value=v;try{await fetch('/api/config/location-engine',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({live_seconds:v})})}catch(e){}updateLocationStatus()}
async function loadLiveSecs(){try{let j=await fetch('/api/config/location-engine').then(r=>r.json());if(j.ok&&j.location_engine&&j.location_engine.live_seconds)$('liveSecs').value=Math.round(j.location_engine.live_seconds)}catch(e){}}
function fmtDur(s){s=Math.round(s||0);if(s<90)return s+'s';if(s<5400)return Math.round(s/60)+' min';if(s<172800)return (s/3600).toFixed(1)+' h';return (s/86400).toFixed(1)+' days'}
function renderApStrip(health){
 let box=$('apStrip');if(!box)return;health=health||[];
 let byKey={};for(let h of health)byKey[macKey(h.mac)]=h;
 let listed=(window.r1APs||[]).filter(a=>a.macAddress);
 let seen=new Set(),chips=[],okN=0;
 let chip=(name,h,a,other)=>{let st,txt,tip=[];
  if(h&&h.lastParAge!=null&&h.lastParAge<=30){st='ok';txt='reporting · '+fmtDur(h.lastParAge)+' ago';if(!other)okN++}
  else if(h&&h.connected){st='warn';txt=h.lastParAge!=null?'connected · silent '+fmtDur(h.lastParAge):'connected · no data yet';tip.push('Connected to the broker but not sending PAR data. The AP may not have received a PAQ yet.')}
  else if(h&&h.event==='disconnected'){st='bad';txt='disconnected '+fmtDur(h.eventAge)+' ago';tip.push('The AP dropped its LBS connection and has not come back. On the AP run: get lbs-server and get lbs wifi0')}
  else{st='bad';txt='never reported';tip.push('This AP has not connected to the LBS broker. Check the venue has Use LBS Server on, and on the AP run: get lbs-server')}
  if(h&&h.pars)tip.push(h.pars+' PAR messages since the app started');
  if(a&&!(a.floorPosition&&a.floorPosition.xPercent!=null))tip.push('Not placed on the floor plan, so its readings cannot be used for locations.');
  if(other)tip.push('Not in the AP list of this venue (it may belong to another venue, or not be placed here).');
  let el=document.createElement('span');el.className='apchip '+st+(other?' other':'');el.title=tip.join(' ');
  el.innerHTML='<span class="dot"></span><b>'+esc(name)+'</b><span class="muted">'+esc(txt)+'</span>'+((a&&!(a.floorPosition&&a.floorPosition.xPercent!=null))?'<span class="muted" title="Not placed on the floor plan">· not placed</span>':'');
  if(a)el.style.cursor='pointer',el.onclick=()=>selectAP(a);
  chips.push(el)};
 for(let a of listed){let k=macKey(a.macAddress);seen.add(k);chip(a.name||a.serialNumber||a.macAddress,byKey[k],a,false)}
 for(let h of health){let k=macKey(h.mac);if(seen.has(k))continue;if(h.lastParAge==null&&!h.connected&&h.event==null)continue;
  // Unknown AP: only worth showing while it is active or recently active (other venues share this broker).
  if(!(h.connected||(h.lastParAge!=null&&h.lastParAge<=300)))continue;chip(h.mac.toUpperCase(),h,null,true)}
 let total=listed.length;
 let unplacedRep=listed.filter(a=>!(a.floorPosition&&a.floorPosition.xPercent!=null)&&(()=>{let h=byKey[macKey(a.macAddress)];return h&&h.lastParAge!=null&&h.lastParAge<=30})());
 let sum=document.createElement('span');sum.className='apsum '+(total&&okN===total?'ok':'bad');
 sum.textContent='APs reporting: '+okN+' of '+total;
 box.innerHTML='';box.appendChild(sum);for(let c of chips)box.appendChild(c);
 if(unplacedRep.length){let w=document.createElement('span');w.style.cssText='color:var(--warn);font-weight:600';w.title='These APs are sending location data but have no position on the selected floor plan, so the app ignores their readings. Place them on the floor plan in RUCKUS One.';w.textContent='\u26a0 '+unplacedRep.length+' reporting AP'+(unplacedRep.length===1?' is':'s are')+' not placed on the floor plan: readings ignored';box.appendChild(w)}}
function strangersToggled(){lsSet('strangers',$('strangersChk').checked?'1':'0');updateLocationStatus()}
function setView(v){showAll=v!==false;locOnly=showAll;fixOnly=v==='fix';$('viewSel').classList.toggle('active',v===false);$('viewAll').classList.toggle('active',v===true);$('viewFix').classList.toggle('active',fixOnly);renderLiveLocations(lastLocs)}
function updateSelLocation(){if(!selected||mode!=='r1')return;let k=macKey(selected.macAddress||selected.mac);let item=lastLocs.find(i=>macKey(i.clientId)===k);let old=$('selLoc');if(old)old.remove();let d=document.createElement('div');d.id='selLoc';d.style.marginTop='8px';
 if(item&&item.location){let l=item.location;d.innerHTML=(item.stale?'<div class="kv"><span class="muted">Status</span><span style="color:var(--warn)">Last known, seen '+fmtAge(item.ageSeconds)+' ago</span></div>':'<div class="kv"><span class="muted">Status</span><span style="color:var(--good)">'+((item.location||{}).kind==='near'||(item.location||{}).kind==='between'?'Live · approximate (not a 3-AP fix)':'Live')+'</span></div>')+(l.kind==='near'||l.kind==='between'?l.kind==='between'?'<div class="kv"><span class="muted">Position</span><span>Between '+esc(apName(l.apIds[0]))+' and '+esc(apName(l.apIds[1]))+'</span></div><div class="muted" style="font-size:11px">Two-AP estimate: on the line between the two APs, nearer the stronger one. Approximate.</div>':'<div class="kv"><span class="muted">Position</span><span>Near '+esc(apName(l.apId))+(l.stranger?' (unlisted device)':'')+'</span></div><div class="muted" style="font-size:11px">Single-AP estimate: shown beside the strongest AP, not triangulated.</div>':'<div class="kv"><span class="muted">Map position</span><span>'+esc(l.xPercent)+'%, '+esc(l.yPercent)+'%</span></div><div class="kv"><span class="muted">Confidence</span><span>'+esc(l.confidence)+'% (accuracy ±'+esc(l.accuracy)+'%)</span></div><div class="kv"><span class="muted">Heard by</span><span>'+esc(l.apCount)+' APs</span></div>')}
 else d.innerHTML='<div class="muted">Not located yet: no AP with a floor position has heard it.</div>';
 if(item&&item.location&&(item.location.unplacedAps||[]).length){let u=item.location.unplacedAps;d.innerHTML+='<div style="margin-top:6px;padding:6px 8px;border:1px solid #e4b35e88;border-radius:8px;font-size:12px"><b style="color:var(--warn)">Also heard by '+u.length+' AP'+(u.length===1?'':'s')+' with no floor position:</b> '+u.map(m=>esc(apName(m))).join(', ')+'. Place '+(u.length===1?'it':'them')+' on the floor plan in RUCKUS One to use '+(u.length===1?'its':'their')+' readings and improve this position.</div>'}
 $('detail').appendChild(d);renderSelAp()}
function renderLiveLocations(locs){
 lastLocs=locs||[];
 document.querySelectorAll('.client.live').forEach(e=>e.remove());
 let sel=selected?macKey(selected.macAddress||selected.mac):'';
 let located=new Set(),stale=new Set();
 for(let item of lastLocs){let l=item.location||{};if(l.xPercent==null||l.yPercent==null)continue;
  if(fixOnly&&(item.stale||l.kind==='near'||l.kind==='between'))continue;   // live fixes only: 3+ APs heard right now
  let k=macKey(item.clientId);located.add(k);if(item.stale)stale.add(k);
  if(!showAll&&k!==sel)continue;
  let name=clientName(item.clientId)||item.clientId;
  let d=document.createElement('div');d.className='client live'+(l.kind==='near'||l.kind==='between'?' near':'')+(l.kind==='between'?' between':'')+(l.stranger?' stranger':'')+(item.stale?' stale':'')+(k===sel?' sel':'');
  d.style.left=l.xPercent+'%';d.style.top=l.yPercent+'%';
  let nearTxt=l.kind==='near'?' · near '+apName(l.apId):l.kind==='between'?' · between '+apName(l.apIds[0])+' & '+apName(l.apIds[1]):'';
  d.title=name+' · '+item.clientId+nearTxt+(item.stale?' · last seen '+fmtAge(item.ageSeconds)+' ago':(l.kind==='near'||l.kind==='between'?'':' · '+l.confidence+'% confidence'));
  d.innerHTML='<span>'+esc(name)+nearTxt+(item.stale?' · last seen '+fmtAge(item.ageSeconds)+' ago':'')+'</span>';
  d.onclick=()=>{scrollCard=true;selectClient(findClient(item.clientId)||{macAddress:item.clientId,hostname:name})};
  $('map').appendChild(d)}
 document.querySelectorAll('#clients .card.tmp').forEach(e=>e.remove());
 let cards=[...document.querySelectorAll('#clients .card')];
 // A located client that RUCKUS One does not list (e.g. not associated) has no card yet: add a temporary one at the top.
 if(sel&&!cards.some(c=>macKey(c.dataset.mac)===sel)){let t=document.createElement('div');t.className='card tmp';t.dataset.mac=selected.macAddress||selected.mac||'';t.innerHTML='<div class="name">'+esc(selected.hostname||selected.alias||selected.macAddress||selected.mac)+'</div><div class="kv"><span class="muted">MAC</span><span>'+esc(selected.macAddress||selected.mac||'')+'</span></div><div class="kv"><span class="muted">AP</span><span>Not associated</span></div>';$('clients').prepend(t);cards.unshift(t)}
 let selCard=null;
 cards.forEach(card=>{let k=macKey(card.dataset.mac);card.classList.toggle('loc',located.has(k));card.style.display=(locOnly&&!located.has(k))?'none':'';card.classList.toggle('stale',stale.has(k));let on=!!sel&&k===sel;card.classList.toggle('sel',on);if(on)selCard=card});
 if(scrollCard&&selCard){selCard.scrollIntoView({block:'nearest',behavior:'smooth'})}
 scrollCard=false;
 updateSelLocation()}
setInterval(()=>{if(mode==='r1' && $('venue').value && $('floor').value)updateLocationStatus()},3000);
// Re-check the RUCKUS One side of the LBS connection so the pill clears once RUCKUS One reconnects.
setInterval(()=>{if(mode==='r1' && $('venue').value && $('venue').value!=='Venue'){let o=$('venue').options[$('venue').selectedIndex];checkLBS($('venue').value,o?o.textContent:'')}},30000);
$('strangersChk').checked=lsGet('strangers')==='1';loadLiveSecs();
changeMode();
</script><div class="modal" id="lbsModal"><div class="dialog"><div style="display:flex;justify-content:space-between;align-items:center"><div><div class="eyebrow">Location Based Services</div><h2 style="margin:4px 0">LBS Receiver</h2></div><button class="btn" onclick="closeLBS()">Close</button></div><p class="muted" style="font-size:13px">RUCKUS One LBS uses <b>TLS 1.2 with TLS-PSK</b>. This build uses Eclipse Mosquitto as the external broker, matching the RUCKUS technical note. RUCKUS One connects to TCP 8883; the local Location Server client uses a private loopback MQTT port.</p><div id="lbsInstall" class="result" style="display:block">Checking Mosquitto…</div><div class="row"><label>Bind IP</label><input id="lbsBind" value="0.0.0.0"></div><div class="row"><label>Advertised / Public IP</label><input id="lbsAdvertised" placeholder="e.g. 203.0.113.10"></div><div class="row"><label>Port</label><input id="lbsPort" value="8883"></div><div class="row"><label>LBS Server Venue Name <span class="muted">(must exactly match RUCKUS One)</span></label><input id="lbsVenueName" placeholder="e.g. lbstest"></div><div class="row"><label>Password <span class="muted">(must exactly match RUCKUS One)</span></label><input id="lbsPass" type="password"></div><div class="row"><label>Local MQTT subscription</label><input id="lbsTopic" value="#"></div><div class="row"><label><input id="lbsTls" type="checkbox" checked disabled> TLS 1.2 / PSK</label></div><div style="display:flex;gap:8px;justify-content:flex-end"><button class="btn" onclick="restartLBS()">Save & Start Broker</button></div><div id="lbsResult" class="result" style="display:none"></div><div id="lbsDiag" class="result" style="display:block;margin-top:10px">Broker diagnostics will appear here after starting. v1.7 follows the documented MGQ → MGR → PAQ → PAR sequence and explicitly enables the RUCKUS TLS-PSK cipher ECDHE-PSK-AES256-CBC-SHA384 and captures MQTT protocol events.</div><div class="muted" style="font-size:12px;margin-top:10px">If Mosquitto is not detected, install the official Eclipse Mosquitto Windows 64-bit package, then restart this application.</div></div></div></body></html>'''
@app.get('/')
def root(): return HTMLResponse(HTML)
