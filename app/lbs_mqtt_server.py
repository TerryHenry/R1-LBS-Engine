import base64, binascii, hashlib, json, os, re, shutil, subprocess, threading, time
from pathlib import Path
from datetime import datetime, timezone

try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

class MosquittoLBSBroker:
    """Manage a Mosquitto broker configured for the RUCKUS One LBS Pre-Engine API.

    RUCKUS One connects to TCP/8883 using TLS 1.2 + TLS-PSK. The documented
    architecture uses an external MQTT broker on 8883 and a local unencrypted
    MQTT client on 1884 for the Location Server application. This class manages
    that broker and the local subscriber/publisher.
    """
    def __init__(self, host='0.0.0.0', port=8883, password='', lbs_server_venue_name='',
                 topic='#', base_dir='.', raw_log=None, on_observations=None):
        self.host = host or '0.0.0.0'
        self.port = int(port or 8883)
        self.password = password or ''
        self.lbs_server_venue_name = (lbs_server_venue_name or '').strip()
        self.topic = topic or '#'
        self.base_dir = Path(base_dir)
        self.raw_log = Path(raw_log) if raw_log else self.base_dir / 'logs' / 'lbs_raw.jsonl'
        self.raw_log.parent.mkdir(parents=True, exist_ok=True)
        self.raw_log_max_bytes = 50 * 1024 * 1024   # lbs_raw.jsonl is rotated above this size
        self.raw_log_keep = 3                       # lbs_raw.jsonl.1 .. .3 are kept (newest is .1)
        self._raw_lock = threading.Lock()
        self._raw_size = 0
        self._raw_retry_at = 0.0
        self.config_dir = self.base_dir / 'logs' / 'lbs_broker'
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.config_file = self.config_dir / 'mosquitto.conf'
        self.psk_file = self.config_dir / 'alpha.txt'
        self.info_file = self.config_dir / 'broker.json'
        self.log_max_bytes = 10 * 1024 * 1024   # rotate mosquitto.log above this size
        self.log_keep = 5                       # mosquitto.log.1 .. .5 are kept (newest is .1)
        self.log_rotations = 0
        self.log_check_seconds = 30
        self.log_rotation_blocked = False
        self.log_file = self.config_dir / 'mosquitto.log'
        self.proc = None
        self.local_client = None
        self.running = False
        self.broker_ready = False
        self.local_connected = False
        self.connected_clients = 0
        self.connection_count = 0
        self.message_count = 0
        self.last_connection = None
        self.last_disconnect = None
        self.last_message = None
        self.last_peer = None
        self.last_event = None
        self.last_error = None
        self.last_stage = 'stopped'
        self.last_connect_result = None
        self.last_tls_failure = None
        self.tls_handshake_failures = 0
        self.last_topics = []
        self.debug_events = []
        self.external_subscriptions = []
        self.external_publishes = []
        self.local_publishes = []
        self.request_burst_count = 0
        self.last_request_burst = None
        self.on_observations = on_observations
        self._stop = threading.Event()
        self._monitor = None
        self._publisher = None
        self.local_port = 1885
        self.external_connected = False
        self.last_external_client = None
        self._request_lock = threading.Lock()
        self._awaiting_mgr = False
        self._external_connect_time = None
        self._conn_ids = set()
        self.ap_last_par = {}      # 'aa:bb:..' -> epoch of the last PAR received from that AP
        self.ap_par_count = {}     # 'aa:bb:..' -> PARs received since this app started
        self.client_events = {}    # 'ap_pub_<MAC>' -> ['connected'|'disconnected', epoch] (survives app restarts via broker.json)
        self.reused_broker = False
        self._broker_pid = None
        self._log_start_pos = 0
        self._paq_sent = False
        self._last_external_subscribe = None
        self.external_subscribe_wait_started = None
        self._enforce_raw_cap()

    @staticmethod
    def _psk_hex(password):
        return password.encode('utf-8').hex()

    @staticmethod
    def _signed_byte(v):
        return v - 256 if v > 127 else v

    @staticmethod
    def _mac(b):
        return ':'.join(f'{x:02x}' for x in b)

    def find_executable(self):
        candidates = []
        w = shutil.which('mosquitto.exe') or shutil.which('mosquitto')
        if w: candidates.append(Path(w))
        pf = os.environ.get('ProgramFiles', r'C:\Program Files')
        pfx86 = os.environ.get('ProgramFiles(x86)', r'C:\Program Files (x86)')
        for root in (pf, pfx86):
            candidates.append(Path(root) / 'mosquitto' / 'mosquitto.exe')
        candidates.append(self.base_dir / 'mosquitto' / 'mosquitto.exe')
        for p in candidates:
            if p.exists(): return str(p)
        return ''


    @staticmethod
    def _list_app_brokers():
        """[(pid, cmdline)] for mosquitto.exe processes launched by a RUCKUS Location Services copy."""
        if os.name != 'nt':
            return []
        try:
            ps = ("Get-CimInstance Win32_Process -Filter \"Name='mosquitto.exe'\" | "
                  "Select-Object ProcessId,CommandLine | ConvertTo-Json -Compress")
            r = subprocess.run(['powershell', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', ps],
                               capture_output=True, text=True, timeout=10, creationflags=subprocess.CREATE_NO_WINDOW)
            raw = (r.stdout or '').strip()
            if not raw:
                return []
            items = json.loads(raw)
            if isinstance(items, dict): items = [items]
            out = []
            for item in items:
                pid = int(item.get('ProcessId') or 0); cmd = str(item.get('CommandLine') or '')
                if pid and 'ruckus-location-services' in cmd.lower():
                    out.append((pid, cmd))
            return out
        except Exception:
            return []

    @staticmethod
    def _norm_path(p):
        return str(p).replace('/', '\\').strip('" ').lower()

    @staticmethod
    def _broker_conf_path(cmd):
        m = re.search(r'-c\s+("([^"]+)"|(\S+))', cmd)
        return (m.group(2) or m.group(3)) if m else ''

    def _is_ours(self, cmd):
        """True if this broker uses this copy's config file, or any app config that listens on our port
        (an older build left running would otherwise block the port). Other copies on other ports are untouched."""
        conf = self._broker_conf_path(cmd)
        if not conf: return False
        if self._norm_path(conf) == self._norm_path(self.config_file): return True
        try:
            text = Path(conf).read_text(encoding='utf-8', errors='replace')
            return re.search(rf'^\s*listener\s+{self.port}\b', text, re.M) is not None
        except Exception:
            return False

    def _reclaim_stale_ruckus_brokers(self, except_pid=None):
        """Stop RUCKUS Location Services brokers that would conflict with ours. The normal Windows
        Mosquitto service (`mosquitto.exe run`) never matches and is never touched."""
        stopped = []
        for pid, cmd in self._list_app_brokers():
            if pid == except_pid or not self._is_ours(cmd): continue
            try:
                subprocess.run(['taskkill', '/PID', str(pid), '/T', '/F'], capture_output=True, text=True, timeout=5,
                               creationflags=subprocess.CREATE_NO_WINDOW)
                stopped.append(pid)
            except Exception:
                pass
        if stopped: time.sleep(0.75)
        return stopped

    # ---- broker lifetime: the broker outlives the web app so RUCKUS One stays connected across restarts ----
    def _fingerprint(self):
        raw = f'{self.host}|{self.port}|{self.lbs_server_venue_name}|{self.password}|tls12-psk-v2'
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()

    def _write_info(self, log_offset=None):
        try:
            self.info_file.write_text(json.dumps({
                'pid': self._broker_pid, 'local_port': self.local_port,
                'fingerprint': self._fingerprint(),
                'log_offset': self._log_start_pos if log_offset is None else log_offset,
                # Connections already known when the log was cut, so a later reattach still sees them.
                'conn_ids': sorted(self._conn_ids), 'events': self.client_events}), encoding='utf-8')
        except Exception as e:
            self.last_error = str(e)

    def _rotate_log(self):
        """Copy-truncate rotation. Mosquitto keeps mosquitto.log open and has no reopen signal on Windows, so
        the file is copied to mosquitto.log.1 (older ones shift up, the oldest is dropped) and then emptied in
        place; Mosquitto opens it in append mode and carries on writing at the new end."""
        try:
            size = self.log_file.stat().st_size
        except OSError:
            return False
        try:
            with self.log_file.open('r+b') as f:
                data = f.read(size)
                for n in range(self.log_keep, 1, -1):
                    src = self.log_file.with_name(f'{self.log_file.name}.{n-1}')
                    dst = self.log_file.with_name(f'{self.log_file.name}.{n}')
                    if src.exists(): os.replace(src, dst)
                first = self.log_file.with_name(f'{self.log_file.name}.1')
                first.write_bytes(data)
                f.seek(size); tail = f.read()          # lines written while copying
                if tail:
                    with first.open('ab') as b: b.write(tail)
                f.seek(0); f.truncate(0)
        except PermissionError:
            self.log_rotation_blocked = True        # broker holds the file open; fine, the log is small now
            return False
        except Exception as e:
            self.last_error = f'Log rotation failed: {e}'
            return False
        self.log_rotations += 1
        self._log_start_pos = 0
        self._write_info(log_offset=0)
        return True

    def _read_info(self):
        try: return json.loads(self.info_file.read_text(encoding='utf-8'))
        except Exception: return {}

    def _try_reuse(self):
        """Reattach to a broker left running by a previous app run if it has the same settings."""
        info = self._read_info()
        pid = int(info.get('pid') or 0); lp = int(info.get('local_port') or 0)
        if not pid or not lp or info.get('fingerprint') != self._fingerprint():
            return False
        alive = [c for p, c in self._list_app_brokers() if p == pid]
        if not alive or self._norm_path(self._broker_conf_path(alive[0])) != self._norm_path(self.config_file):
            return False
        import socket
        try:
            with socket.create_connection(('127.0.0.1', lp), timeout=1.0):
                pass
        except Exception:
            return False
        self.local_port = lp; self._broker_pid = pid; self.reused_broker = True
        # Connections made while the web app was down never reached this process: rebuild them from the log.
        self._conn_ids = set(info.get('conn_ids') or [])
        self.client_events = {k: v for k, v in (info.get('events') or {}).items() if isinstance(v, list) and len(v) == 2}
        self._replay_connections(int(info.get('log_offset') or 0))
        try: size = self.log_file.stat().st_size
        except OSError: size = 0
        self._log_start_pos = size
        if size > self.log_max_bytes: self._rotate_log()
        self._write_info()
        return True

    def ap_health(self):
        """One row per access point the broker has ever seen: is it connected, and when did it last send a PAR."""
        now = time.time(); rows = {}
        def row(mac): return rows.setdefault(mac, {'mac': mac, 'connected': False, 'lastParAge': None, 'pars': 0, 'event': None, 'eventAge': None})
        for cid, (kind, epoch) in self.client_events.items():
            h = cid[7:]
            if len(h) != 12: continue
            r = row(':'.join(h[i:i+2] for i in range(0, 12, 2)).lower())
            r['event'] = kind; r['eventAge'] = max(0, round(now - float(epoch)))
        for cid in self._conn_ids:
            h = cid[7:] if cid.startswith('ap_pub_') else ''
            if len(h) == 12: row(':'.join(h[i:i+2] for i in range(0, 12, 2)).lower())['connected'] = True
        for mac, t in self.ap_last_par.items():
            r = row(mac); r['lastParAge'] = max(0, round(now - t)); r['pars'] = self.ap_par_count.get(mac, 0)
        return sorted(rows.values(), key=lambda r: r['mac'])

    def client_counts(self):
        """(access points, RUCKUS One) currently connected to the broker. APs connect as ap_pub_<MAC>;
        anything else that is not our own local subscriber is RUCKUS One's cloud client."""
        aps = sum(1 for c in self._conn_ids if c.startswith('ap_pub_'))
        return aps, len(self._conn_ids) - aps

    def _track_connections(self, s):
        """Maintain the set of connected clients from a Mosquitto log line. Returns True if it changed."""
        ev = re.match(r'(\d+): ', s)
        epoch = int(ev.group(1)) if ev else int(time.time())
        m = re.search(r'New client connected from \S+ as (\S+)', s)
        if m:
            cid = m.group(1)
            if cid.startswith('ap_pub_'): self.client_events[cid] = ['connected', epoch]
            if cid != 'ruckus-location-ls':
                self._conn_ids.add(cid); self.last_external_client = cid
            return True
        m = re.search(r'Client (\S+) .*?(?:disconnected|closed its connection|exceeded timeout)', s)
        if m and m.group(1) != 'ruckus-location-ls':
            self._conn_ids.discard(m.group(1))
            if m.group(1).startswith('ap_pub_'): self.client_events[m.group(1)] = ['disconnected', epoch]
            return True
        return False

    def _replay_connections(self, offset):
        try:
            size = self.log_file.stat().st_size
            if offset > size: offset = 0                       # log was cut or removed since broker.json was written
            with self.log_file.open('r', encoding='utf-8', errors='replace') as f:
                f.seek(max(0, offset))
                for line in f:
                    if 'New client connected' in line or 'isconnect' in line or 'closed its connection' in line or 'exceeded timeout' in line:
                        self._track_connections(line.strip())
        except Exception as e:
            self.last_error = str(e)
        self.connected_clients = len(self._conn_ids)
        if self._conn_ids:
            self.external_connected = True
            self._external_connect_time = time.time()

    def _spawn_detached(self, exe):
        """Start Mosquitto so it is NOT a child of this process: stop.bat's `taskkill /T` and the end of this
        process leave it running. Windows: launched through a short-lived PowerShell that exits immediately."""
        conf = str(self.config_file)
        if os.name != 'nt':
            p = subprocess.Popen([exe, '-c', conf], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 start_new_session=True)
            return p.pid
        q = lambda v: v.replace("'", "''")
        ps = (f"$p = Start-Process -FilePath '{q(exe)}' -ArgumentList '-c','\"{q(conf)}\"' "
              f"-WindowStyle Hidden -PassThru; $p.Id")
        r = subprocess.run(['powershell', '-NoProfile', '-ExecutionPolicy', 'Bypass', '-Command', ps],
                           capture_output=True, text=True, timeout=20, creationflags=subprocess.CREATE_NO_WINDOW)
        lines = [x.strip() for x in (r.stdout or '').splitlines() if x.strip().isdigit()]
        if not lines:
            raise RuntimeError(f'Could not start Mosquitto: {(r.stderr or r.stdout or "no output").strip()[:300]}')
        return int(lines[-1])

    def _choose_local_port(self):
        """Choose a free loopback MQTT port without touching any system broker."""
        import socket
        for port in range(1885, 1896):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind(('127.0.0.1', port))
                return port
            except OSError:
                continue
            finally:
                sock.close()
        raise RuntimeError('No free local MQTT port found in 127.0.0.1:1885-1895.')

    def _write_config(self):
        if not self.lbs_server_venue_name:
            raise RuntimeError('LBS Server Venue Name is required. It must exactly match the RUCKUS One LBS profile venue name.')
        if not (2 <= len(self.lbs_server_venue_name) <= 16):
            raise RuntimeError('LBS Server Venue Name must be 2-16 characters.')
        if not self.password or not (8 <= len(self.password) <= 63):
            raise RuntimeError('LBS password must be 8-63 characters.')
        identity = self.lbs_server_venue_name
        psk_hex = self._psk_hex(self.password)
        self.psk_file.write_text(f'{identity}:{psk_hex}\n', encoding='ascii')
        # RUCKUS One uses TLS-PSK on 8883; the local Location Server client uses
        # a private loopback MQTT listener. We never modify or stop a system broker.
        conf = f'''# Generated by RUCKUS Location Services\n# RUCKUS One LBS Pre-Engine API broker\n\nconnection_messages true\n# Per-message debug logging made the log grow by megabytes a day and cannot be rotated while the broker\n# runs on Windows, so log only what the app parses: connections, subscriptions, warnings and errors.\nlog_type error\nlog_type warning\nlog_type notice\nlog_type information\nlog_type subscribe\nlog_type unsubscribe\nlog_dest file {self.log_file.as_posix()}\n\n# Local Location Server subscriber: unencrypted loopback only.\nlistener {self.local_port} 127.0.0.1\nlistener_allow_anonymous true\n\n# RUCKUS One external LBS listener: TLS-PSK on TCP/8883.\nlistener {self.port} {self.host}\npsk_hint alpha\npsk_file {self.psk_file.as_posix()}\ntls_version tlsv1.2\n# RUCKUS One LBS/Pre-Engine uses this TLS 1.2 PSK cipher.\n# Explicit selection avoids OpenSSL 3.x default cipher filtering producing\n# a "no shared cipher" failure during the RUCKUS PSK handshake.\nciphers ECDHE-PSK-AES256-CBC-SHA384\n# RUCKUS One authenticates at the TLS-PSK layer; it does not send an MQTT username/password.\nlistener_allow_anonymous true\n'''
        self.config_file.write_text(conf, encoding='utf-8')

    def _parse_broker_log(self, line):
        s=line.strip()
        low=s.lower()
        self.last_event=s[-500:]
        if s:
            self.debug_events.append(s[-1000:])
            self.debug_events=self.debug_events[-100:]
        # Mosquitto emits these messages for the exact RUCKUS connection path.
        if 'new connection from' in low and f'on port {self.port}' in low:
            self.connection_count += 1
            self.last_connection=datetime.now(timezone.utc).isoformat()
            m=re.search(r'New connection from ([^ ]+)', s, re.I)
            if m: self.last_peer=m.group(1)
            self.last_stage='tcp_connected'
        if 'new client connected' in low:
            self.last_stage='mqtt_authenticated'
            self.last_connect_result='accepted'
            # The RUCKUS One client can connect after our local Location Server
            # client has already published its startup MGQ/PAQ requests.
            # In that case those requests are already gone and RUCKUS One will
            # sit idle, sending only MQTT keepalives. Re-issue the requests as
            # soon as the external RUCKUS client is authenticated.
            if 'ruckus-location-ls' not in low:
                self.external_connected = True
                m2 = re.search(r'as ([^ ]+)', s, re.I)
                if m2:
                    self.last_external_client = m2.group(1)
                # Wait for RUCKUS One to subscribe before publishing MGQ.
                # The technical note shows RUCKUS One subscribing to MGQ/PAQ/STQ
                # immediately after CONNECT; publishing before that subscription
                # can discard the request. The actual trigger is handled when
                # Mosquitto logs the external SUBSCRIBE below.
                self.external_subscribe_wait_started = datetime.now(timezone.utc).isoformat()
                self._external_connect_time = time.time()
                self.last_stage='mqtt_authenticated_waiting_subscribe'
        if 'New client connected' in s or 'isconnect' in s or 'closed its connection' in s or 'exceeded timeout' in s:
            if self._track_connections(s):
                self.connected_clients = len(self._conn_ids)
                if not self._conn_ids: self.external_connected = False
            if 'New client connected' not in s and 'ruckus-location-ls' not in low:
                self.last_disconnect=datetime.now(timezone.utc).isoformat()
        if 'psk identity not found' in low or 'bad record mac' in low:
            self.tls_handshake_failures += 1
            self.last_tls_failure=datetime.now(timezone.utc).isoformat()
            self.last_stage='tls_psk_failed'
            self.last_error=s[-500:]
        if 'no shared cipher' in low:
            self.tls_handshake_failures += 1
            self.last_tls_failure=datetime.now(timezone.utc).isoformat()
            self.last_stage='tls_no_shared_cipher'
            self.last_error=s[-500:]
        if 'wrong version number' in low:
            self.tls_handshake_failures += 1
            self.last_tls_failure=datetime.now(timezone.utc).isoformat()
            self.last_stage='tls_version_failed'
            self.last_error=s[-500:]
        sub_m = re.search(r'\s(\S+) (\d) (\S*/LS/(?:MGQ|PAQ|STQ))$', s)
        if 'received subscribe' in low or sub_m:
            self.last_stage='mqtt_subscribed'
            if 'ruckus-location-ls' not in low:
                self.external_subscriptions.append(s[-1000:]); self.external_subscriptions=self.external_subscriptions[-50:]
                self._last_external_subscribe=datetime.now(timezone.utc).isoformat()
                self.external_subscribe_wait_started=None
                # RUCKUS One has now subscribed to the request topics. Publish
                # MGQs only after this point so the cloud client can receive them.
                threading.Thread(target=self._send_mgqs_after_external_subscribe, daemon=True).start()
        if 'received publish' in low:
            self.last_stage='publish_received'
            if 'ruckus-location-ls' not in low:
                self.external_publishes.append(s[-1000:]); self.external_publishes=self.external_publishes[-50:]

    def _monitor_log(self):
        # Mosquitto appends to its log across runs. Start after the old content
        # so a previous run's RUCKUS connection is not replayed as a live one.
        pos=getattr(self,'_log_start_pos',0)
        last_check=time.monotonic()
        while not self._stop.is_set():
            try:
                if self.log_file.exists():
                    size=self.log_file.stat().st_size
                    if size < pos: pos=0          # log was truncated (rotation, or emptied by hand)
                    with self.log_file.open('r',encoding='utf-8',errors='replace') as f:
                        f.seek(pos)
                        for line in f:
                            self._parse_broker_log(line)
                        pos=f.tell()
                    # Rotate only once everything written so far has been parsed.
                    if pos >= size and size > self.log_max_bytes and not self.log_rotation_blocked and time.monotonic()-last_check >= self.log_check_seconds:
                        last_check=time.monotonic()
                        if self._rotate_log(): pos=0
            except Exception as e:
                self.last_error=str(e)
            time.sleep(0.25)

    def _raw_backup(self, n):
        return self.raw_log.with_name(f'{self.raw_log.name}.{n}')

    def _shift_raw_backups(self):
        for n in range(self.raw_log_keep, 1, -1):
            src = self._raw_backup(n - 1)
            if src.exists(): os.replace(src, self._raw_backup(n))

    def _enforce_raw_cap(self):
        """On startup: size the raw log from disk. A file already over the cap (it used to grow without limit)
        is cut down to its newest cap-worth of lines, kept as lbs_raw.jsonl.1, and the oversized original removed."""
        try:
            size = self.raw_log.stat().st_size
        except OSError:
            self._raw_size = 0; return
        if size <= self.raw_log_max_bytes:
            self._raw_size = size; return
        try:
            with self.raw_log.open('rb') as f:
                f.seek(size - self.raw_log_max_bytes); f.readline()      # start on a whole line
                tail = f.read()
            self._shift_raw_backups()
            self._raw_backup(1).write_bytes(tail)
            self.raw_log.unlink()
            self._raw_size = 0
        except Exception as e:
            self._raw_size = size; self.last_error = f'Raw log trim failed: {e}'

    def _rotate_raw(self):
        try:
            self._shift_raw_backups()
            os.replace(self.raw_log, self._raw_backup(1))
            self._raw_size = 0
        except Exception:
            self._raw_retry_at = time.time() + 60        # e.g. the file is open in another program; keep logging, retry later

    @staticmethod
    def _printable_text(payload):
        try:
            t = payload.decode('utf-8')
        except Exception:
            return None
        return t if all(c >= ' ' or c in '\t\n\r' for c in t) else None

    def _append_raw(self, topic, payload, source='mosquitto-local'):
        rec={'received_at':datetime.now(timezone.utc).isoformat(),'topic':topic,
             'source':source,'payload_base64':base64.b64encode(payload).decode('ascii'),
             'payload_length':len(payload)}
        txt=self._printable_text(payload)
        if txt is not None: rec['payload_utf8']=txt     # binary payloads (PAR, MGR) are fully described by payload_base64
        data=(json.dumps(rec,separators=(',',':'))+'\n').encode('utf-8')
        with self._raw_lock:
            if self._raw_size + len(data) > self.raw_log_max_bytes and time.time() >= self._raw_retry_at:
                self._rotate_raw()
            with self.raw_log.open('ab') as f:
                f.write(data)
            self._raw_size += len(data)
        self.message_count += 1
        if topic.endswith('/PAR'):
            parts = topic.split('/')
            h = parts[-2] if len(parts) >= 2 else ''
            if len(h) == 12:
                mac = ':'.join(h[i:i+2] for i in range(0, 12, 2)).lower()
                self.ap_last_par[mac] = time.time(); self.ap_par_count[mac] = self.ap_par_count.get(mac, 0) + 1
            self.last_stage='publish_received'
            self.external_publishes.append(f"{topic} ({len(payload)} bytes)"); self.external_publishes=self.external_publishes[-50:]
        self.last_message=rec['received_at']
        self.last_topics.append(topic); self.last_topics=self.last_topics[-20:]
        obs=self.decode_par(topic,payload)
        if obs and self.on_observations:
            try: self.on_observations(obs)
            except Exception as e: self.last_error=str(e)

    def _on_local_connect(self, client, userdata, flags, reason_code, properties=None):
        rc = getattr(reason_code, 'value', reason_code)
        if int(rc)==0:
            self.local_connected=True
            self.last_stage='location_app_connected'
            client.subscribe(self.topic or '#', qos=0)
            self._send_requests(client)
        else:
            self.local_connected=False
            self.last_error=f'Local MQTT broker connection failed: {reason_code}'

    def _on_local_disconnect(self, client, userdata, flags, reason_code, properties=None):
        self.local_connected=False
        if self.running: self.last_stage='location_app_disconnected'

    def _on_local_message(self, client, userdata, msg):
        self._append_raw(msg.topic, msg.payload)
        # RUCKUS Technical Note: after the LS application publishes MGQ,
        # RUCKUS One returns an MGR (message type 0x02, subtype 0x01 or 0x07).
        # Only after receiving MGR do we send the PAQ requests that put the AP
        # radios into PASSIVE TRACKING mode.
        if len(msg.payload) >= 4 and msg.payload[0] == 0x02 and msg.payload[1] in (0x01, 0x07):
            self.last_stage='mgr_received'
            self._awaiting_mgr=False
            if not self._paq_sent:
                self._send_paq(client)

    def _send_mgqs(self, client):
        v=self.lbs_server_venue_name
        if not v: return
        with self._request_lock:
            mgq=f'3.0/LOC/{v}/LS/MGQ'
            payloads=[bytes.fromhex('01 01 00 00'), bytes.fromhex('01 07 00 00')]
            for payload in payloads:
                r=client.publish(mgq, payload, qos=2, retain=True)
                self.local_publishes.append(f'{mgq} {payload.hex()} rc={getattr(r, "rc", None)} mid={getattr(r, "mid", None)}')
            self.local_publishes=self.local_publishes[-50:]
            self.request_burst_count += 1
            self.last_request_burst=datetime.now(timezone.utc).isoformat()
            self._awaiting_mgr=True
            self._paq_sent=False
            self.last_stage='mgq_published_waiting_mgr'

    def _send_paq(self, client):
        v=self.lbs_server_venue_name
        if not v or self._paq_sent: return
        with self._request_lock:
            paq=f'3.0/LOC/{v}/LS/PAQ'
            payloads=[bytes.fromhex('03 01 00 07 06 05 a0 01 00 00 00'),
                      bytes.fromhex('03 01 00 07 06 05 a0 02 00 00 00')]
            for payload in payloads:
                r=client.publish(paq, payload, qos=2, retain=True)
                self.local_publishes.append(f'{paq} {payload.hex()} rc={getattr(r, "rc", None)} mid={getattr(r, "mid", None)}')
            self.local_publishes=self.local_publishes[-50:]
            self._paq_sent=True
            self.last_stage='paq_published_waiting_par'

    def _send_mgqs_after_external_subscribe(self):
        # RUCKUS One subscribes to MGQ, PAQ and STQ in three SUBSCRIBEs (and APs
        # may connect too). Coalesce them into one MGQ burst, sent after the last.
        with self._request_lock:
            self._subscribe_seq = getattr(self, '_subscribe_seq', 0) + 1
            seq = self._subscribe_seq
        time.sleep(1.0)
        if seq != self._subscribe_seq:
            return
        try:
            if self.local_client and self.local_connected and self.external_connected:
                self._send_mgqs(self.local_client)
        except Exception as e:
            self.last_error=str(e)

    def _send_requests(self, client):
        # Protocol-correct sequence from the RUCKUS technical note:
        # 1) publish MGQ controller + venue requests;
        # 2) wait for MGR response;
        # 3) publish PAQ 2.4/5 GHz;
        # 4) receive PAR continuously.
        self._send_mgqs(client)

    def _send_requests_after_external_connect(self):
        # Retained for compatibility with older callers; v1.5 uses the
        # external SUBSCRIBE event as the precise trigger for MGQ.
        self._send_mgqs_after_external_subscribe()

    def _publisher_loop(self):
        # PAQ requests put AP radios into passive tracking for 1440 minutes.
        # The technical note requires a new PAQ before that duration expires;
        # do not emit repeated MGQ/PAQ bursts every minute. Re-arm only after
        # an external reconnect/subscription sequence.
        last_paq = time.time()
        last_mgq_retry = 0.0
        while not self._stop.wait(5):
            now = time.time()
            try:
                ready = self.local_client and self.local_connected and self.external_connected
                # Fallback: RUCKUS One connected but no SUBSCRIBE was seen within
                # 10 s, or no MGR has come back. Publish MGQ anyway (retained, so
                # RUCKUS One still gets it when it subscribes) and retry every 60 s.
                if ready and not self._paq_sent:
                    ct = self._external_connect_time
                    if ct and now - ct > 10 and now - last_mgq_retry > 60:
                        last_mgq_retry = now
                        self.last_stage = 'mgq_fallback_publish'
                        self._send_mgqs(self.local_client)
                # Renew PAQ every 12h, well inside the 1,440 minute duration.
                if self._paq_sent and now - last_paq > 12*3600 and ready:
                    self._paq_sent = False
                    self._send_paq(self.local_client)
                    last_paq = now
            except Exception as e:
                self.last_error = str(e)

    def diagnostics(self):
        return {
            'external_connected': self.external_connected,
            'ap_health': self.ap_health(),
            'external_client': self.last_external_client,
            'external_subscriptions': self.external_subscriptions[-20:],
            'external_publishes': self.external_publishes[-20:],
            'local_publishes': self.local_publishes[-20:],
            'request_burst_count': self.request_burst_count,
            'last_request_burst': self.last_request_burst,
            'awaiting_mgr': self._awaiting_mgr,
            'paq_sent': self._paq_sent,
            'last_external_subscribe': self._last_external_subscribe,
            'external_subscribe_wait_started': self.external_subscribe_wait_started,
            'last_event': self.last_event,
            'last_stage': self.last_stage,
            'last_error': self.last_error,
            'last_topics': self.last_topics[-20:],
            'message_count': self.message_count,
        }

    def decode_par(self, topic, payload):
        # PAR fixed header is 22 bytes. Payload then contains 11 bytes per client.
        if len(payload) < 22 or payload[0] != 0x04:
            return []
        subtype=payload[1]
        length=int.from_bytes(payload[2:4],'big')
        timestamp=int.from_bytes(payload[4:8],'big')
        sequence=int.from_bytes(payload[8:12],'big')
        ap_mac=self._mac(payload[12:18])
        band=payload[18]
        client_count=int.from_bytes(payload[20:22],'big')
        expected=min(client_count, max(0,(len(payload)-22)//11))
        out=[]
        p=22
        for _ in range(expected):
            mac=self._mac(payload[p:p+6]); phy=self._signed_byte(payload[p+6])
            rssi_bytes=payload[p+7:p+10]; snr=self._signed_byte(payload[p+10])
            rssis=[self._signed_byte(x) for x in rssi_bytes if x != 0x80]
            if rssis:
                # Use strongest chain as the AP-level RSSI observation. Keep all
                # chains in the record for diagnostics.
                rssi=max(rssis)
                out.append({'client_id':mac,'ap_mac':ap_mac,'ap_id':ap_mac,
                            'rssi':rssi,'rssi_chains':rssis,'snr':snr,
                            'phy_info':phy,'band':band,'timestamp':timestamp,
                            'sequence':sequence,'topic':topic})
            p += 11
        return out

    def start(self):
        if self.running: return {'ok':True,'message':'LBS broker already running.'}
        if mqtt is None:
            raise RuntimeError('paho-mqtt is required. Start the application once so dependencies install, then retry.')
        exe=self.find_executable()
        if not exe:
            raise RuntimeError('Mosquitto broker is not installed. Install Eclipse Mosquitto for Windows (64-bit), then click Save & Start Receiver again. The RUCKUS LBS protocol requires TLS-PSK, which the embedded Python TLS server cannot provide.')
        # Never stop or manage an existing Windows Mosquitto service.
        # If a broker from a previous run of this app is still up with the same settings, reattach to it:
        # restarting only the web app then does not drop RUCKUS One's connection (it takes RUCKUS One
        # many minutes to reconnect). Otherwise clear conflicting app brokers and start a detached one.
        self._stop.clear()
        self._conn_ids = set(); self.connected_clients = 0; self.reused_broker = False
        import socket
        def _port_open(port, host):
            try:
                with socket.create_connection((host, int(port)), timeout=0.5):
                    return True
            except Exception:
                return False
        if not self._try_reuse():
            self._reclaim_stale_ruckus_brokers()
            self.local_port = self._choose_local_port()
            self._write_config()
            try: self._log_start_pos = self.log_file.stat().st_size
            except OSError: self._log_start_pos = 0
            if self._log_start_pos > self.log_max_bytes: self._rotate_log()
            self._broker_pid = self._spawn_detached(exe)
            self._write_info()
        self.proc = None
        self.running=True; self.broker_ready=False; self.last_stage='broker_starting' if not self.reused_broker else 'broker_reattached'; self.last_error=None
        self._monitor=threading.Thread(target=self._monitor_log,daemon=True); self._monitor.start()
        # Give broker enough time to start on slower Windows systems, and report
        # the broker's own log tail if it exits before binding.
        # Only probe the local loopback listener. Do NOT open a test TCP
        # connection to 8883: that creates a fake localhost TLS handshake in
        # the broker log and can be mistaken for RUCKUS One activity.
        deadline = time.time() + 6.0
        while time.time() < deadline:
            if _port_open(self.local_port, '127.0.0.1'):
                break
            time.sleep(0.25)
        if not _port_open(self.local_port, '127.0.0.1'):
            try:
                log_tail = self.log_file.read_text(encoding='utf-8', errors='replace')[-2000:]
            except Exception:
                log_tail = ''
            detail = log_tail.strip() or self.last_error or 'Mosquitto did not bind the local listener.'
            self.last_stage='listener_error'
            self.last_error=f'Mosquitto did not open local MQTT 127.0.0.1:{self.local_port}. Broker log tail: {detail}'
            self.stop(kill_broker=True)
            raise RuntimeError(self.last_error)
        self.local_client=mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id='ruckus-location-ls')
        self.local_client.on_connect=self._on_local_connect
        self.local_client.on_disconnect=self._on_local_disconnect
        self.local_client.on_message=self._on_local_message
        last_exc=None
        for _ in range(10):
            try:
                self.local_client.connect('127.0.0.1',self.local_port,keepalive=60)
                break
            except Exception as e:
                last_exc=e; time.sleep(0.5)
        else:
            self.stop()
            raise RuntimeError(f'Local Mosquitto broker did not open TCP/{self.local_port}: {last_exc}')
        self.local_client.loop_start()
        self.broker_ready=True
        self._publisher=threading.Thread(target=self._publisher_loop,daemon=True); self._publisher.start()
        self.last_stage='listening_tls_psk'
        return {'ok':True,'message':f'Mosquitto LBS broker listening on {self.host}:{self.port} (TLS-PSK).','running':True}

    def stop(self, kill_broker=False):
        """Disconnect the local client. The Mosquitto broker is left running (so RUCKUS One stays connected)
        unless kill_broker is set, which is used when the broker's settings change or it is unhealthy."""
        self.running=False; self._stop.set()
        self.external_connected=False; self.last_external_client=None
        try:
            if self.local_client:
                self.local_client.loop_stop(); self.local_client.disconnect()
        except Exception: pass
        self.local_client=None; self.local_connected=False
        if kill_broker:
            pid = self._broker_pid
            if pid:
                try: subprocess.run(['taskkill','/PID',str(pid),'/T','/F'],capture_output=True,text=True,timeout=5,
                                    creationflags=subprocess.CREATE_NO_WINDOW if os.name=='nt' else 0)
                except Exception: pass
            self._reclaim_stale_ruckus_brokers()
            try: self.info_file.unlink()
            except OSError: pass
            self._broker_pid=None
        self.proc=None; self.last_stage='stopped'; self.connected_clients=0; self._conn_ids=set()

    @property
    def last_error_text(self): return self.last_error
