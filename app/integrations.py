import json, ssl
from pathlib import Path
from datetime import datetime, timezone

BASE = Path(__file__).resolve().parent.parent
RAW_LOG = BASE / 'logs' / 'lbs_raw.jsonl'
RAW_LOG.parent.mkdir(exist_ok=True)

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

class RuckusOneClient:
    def __init__(self, base_url, auth_url, tenant_id, client_id, client_secret):
        self.base_url = base_url.rstrip('/')
        self.auth_url = auth_url.rstrip('/')
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = None
        self._expires_at = 0

    async def token(self):
        import httpx, time
        if self._token and time.time() < self._expires_at - 60:
            return self._token
        url = f'{self.auth_url}/oauth2/token/{self.tenant_id}'
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            r = await c.post(url, data={'grant_type':'client_credentials','client_id':self.client_id,'client_secret':self.client_secret}, headers={'Accept':'application/json'})
        if not r.is_success:
            raise RuntimeError(f'Authentication failed ({r.status_code}): {r.text[:600]}')
        body = r.json()
        self._token = body.get('access_token')
        if not self._token:
            raise RuntimeError('Authentication response did not contain access_token')
        self._expires_at = time.time() + int(body.get('expires_in') or 3600)
        return self._token

    async def request(self, method, path, **kwargs):
        import httpx
        tok = await self.token()
        headers = kwargs.pop('headers', {})
        headers.update({'Authorization':f'Bearer {tok}','Accept':'application/json'})
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            r = await c.request(method, self.base_url + path, headers=headers, **kwargs)
        if r.status_code == 401:
            self._token = None
            tok = await self.token()
            headers['Authorization'] = f'Bearer {tok}'
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
                r = await c.request(method, self.base_url + path, headers=headers, **kwargs)
        return r

    async def get(self, path, **kwargs): return await self.request('GET', path, **kwargs)
    async def post(self, path, **kwargs): return await self.request('POST', path, **kwargs)

class LBSReceiver:
    def __init__(self, host='0.0.0.0', port=8883, username='', password='', tls=True, ca_file='', client_cert='', client_key='', topic='#', on_observations=None):
        self.host,self.port=host,port; self.username,self.password=username,password
        self.tls=tls; self.ca_file,self.client_cert,self.client_key=ca_file,client_cert,client_key; self.topic=topic
        self.client=None; self.connected=False; self.on_observations=on_observations
    def start(self):
        if mqtt is None: raise RuntimeError('paho-mqtt is not installed')
        self.client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='ruckus-location')
        if self.username: self.client.username_pw_set(self.username,self.password)
        if self.tls:
            ctx=ssl.create_default_context(cafile=self.ca_file or None)
            if self.client_cert and self.client_key: ctx.load_cert_chain(self.client_cert,self.client_key)
            self.client.tls_set_context(ctx)
        self.client.on_connect=self._on_connect; self.client.on_message=self._on_message
        self.client.connect(self.host,self.port,keepalive=60); self.client.loop_start()
    def _on_connect(self,client,userdata,flags,reason_code,properties=None):
        self.connected=(reason_code==0)
        if self.connected: client.subscribe(self.topic,qos=0)
    def _on_message(self,client,userdata,msg):
        rec={'received_at':datetime.now(timezone.utc).isoformat(),'topic':msg.topic,'qos':msg.qos,'payload_utf8':msg.payload.decode('utf-8','replace')}
        with RAW_LOG.open('a',encoding='utf-8') as f: f.write(json.dumps(rec)+'\n')
        try:
            obs=parse_lbs_observations(rec['payload_utf8'])
            if obs and self.on_observations: self.on_observations(obs)
        except Exception: pass
    def stop(self):
        if self.client: self.client.loop_stop(); self.client.disconnect(); self.connected=False


def parse_lbs_observations(payload):
    """Best-effort parser for common LBS JSON shapes.
    The supplied RUCKUS One OpenAPI does not define the telemetry payload, so this
    intentionally accepts several obvious field names without claiming a vendor schema.
    Returns [{client_id, ap_id, rssi, snr?}] or [].
    """
    try:
        obj=json.loads(payload) if isinstance(payload,(str,bytes,bytearray)) else payload
    except Exception:
        return []
    rows=obj if isinstance(obj,list) else None
    if rows is None and isinstance(obj,dict):
        for k in ('observations','clients','measurements','data','records','items'):
            if isinstance(obj.get(k),list): rows=obj[k]; break
        if rows is None: rows=[obj]
    if not isinstance(rows,list): return []
    out=[]
    for r in rows:
        if not isinstance(r,dict): continue
        client=r.get('clientId') or r.get('clientID') or r.get('client_id') or r.get('macAddress') or r.get('mac') or r.get('stationMac') or r.get('deviceId')
        ap=r.get('apId') or r.get('apID') or r.get('ap_id') or r.get('serialNumber') or r.get('apSerial') or r.get('serial') or r.get('bssid')
        sig=r.get('rssi')
        if sig is None and isinstance(r.get('signalStatus'),dict): sig=r['signalStatus'].get('rssi')
        if client is None or ap is None or sig is None: continue
        try: sig=float(sig)
        except Exception: continue
        out.append({'client_id':str(client),'ap_id':str(ap),'rssi':sig,'snr':r.get('snr')})
    return out
