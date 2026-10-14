import math, time
from collections import defaultdict


def norm_mac(v):
    """Normalise a MAC/serial for lookup: lowercase, colon-separated when it is a MAC."""
    s=str(v or '').strip().lower()
    h=s.replace(':','').replace('-','').replace('.','')
    if len(h)==12 and all(c in '0123456789abcdef' for c in h):
        return ':'.join(h[i:i+2] for i in range(0,12,2))
    return s


def _lookup_ap(ap, ap_positions):
    if ap in ap_positions: return ap_positions[ap]
    n=norm_mac(ap)
    if n in ap_positions: return ap_positions[n]
    # PAR reports the AP/radio MAC, which can differ from the base MAC that
    # RUCKUS One lists in only the last byte. Accept a unique 5-byte prefix match.
    if n.count(':')==5:
        pre=n.rsplit(':',1)[0]
        hits={v for k,v in ap_positions.items() if norm_mac(k).startswith(pre+':')}
        if len(hits)==1: return next(iter(hits))
    return None


def estimate_position(observations, ap_positions, reference_rssi=-45.0, path_loss_exponent=2.2, max_age=30.0):
    """Estimate client position from >=3 AP RSSI observations.
    This is a first-pass RSSI weighted centroid, not vendor-specific multilateration.
    observations: [{ap_id, rssi}, ...]
    ap_positions: {ap_id: (x_percent, y_percent)}
    """
    pts=[]
    # Keep only the newest, fresh reading per AP. Previously up to 40 historical
    # readings were used, so one AP heard three times counted as "3 APs" and
    # stale readings skewed the result.
    now=time.time(); latest={}
    for o in observations:
        try:
            ap=norm_mac(o.get('ap_id') or o.get('serialNumber') or o.get('serial') or o.get('bssid') or '')
            ts=float(o.get('timestamp') or now)
            if abs(now-ts)>max_age: continue
            if ap not in latest or ts>=latest[ap][0]: latest[ap]=(ts,o)
        except Exception: continue
    observations=[v[1] for v in latest.values()]
    n=max(float(path_loss_exponent or 2.2), 1.0)
    for o in observations:
        try:
            ap=norm_mac(o.get('ap_id') or o.get('serialNumber') or o.get('serial') or o.get('bssid') or '')
            rssi=float(o.get('rssi'))
            pos=_lookup_ap(ap, ap_positions)
            if pos is None or not math.isfinite(rssi): continue
            x,y=pos
            # Relative distance. Absolute meters require calibration; use normalized weights.
            d=10 ** ((reference_rssi-rssi)/(10*n))
            w=1.0/max(d,0.05)**2
            pts.append((x,y,w,rssi,ap,d))
        except Exception:
            continue
    if len(pts)<3: return None
    sw=sum(p[2] for p in pts)
    x=sum(p[0]*p[2] for p in pts)/sw
    y=sum(p[1]*p[2] for p in pts)/sw
    # Spread of the contributing APs gives a useful relative confidence/accuracy indicator.
    residual=sum(math.hypot(p[0]-x,p[1]-y)*p[2] for p in pts)/sw
    accuracy=max(2.0, min(20.0, residual*0.55))
    confidence=max(20.0, min(98.0, 100.0/(1.0+residual/8.0) + min(20,len(pts))*2.0))
    return {'xPercent':round(max(0,min(100,x)),2),'yPercent':round(max(0,min(100,y)),2),
            'accuracy':round(accuracy,1),'confidence':round(confidence),
            'method':'RSSI weighted centroid','apCount':len(pts),
            'observations':[{'apId':p[4],'rssi':p[3]} for p in pts],
            'timestamp':time.time()}


def smooth(previous, current, alpha=0.35):
    if not previous: return current
    a=max(0.05,min(1.0,float(alpha or 0.35)))
    out=dict(current)
    out['xPercent']=round(previous['xPercent']*(1-a)+current['xPercent']*a,2)
    out['yPercent']=round(previous['yPercent']*(1-a)+current['yPercent']*a,2)
    return out
