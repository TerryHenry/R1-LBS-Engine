# RUCKUS Location Services v1.4

Windows-native RUCKUS One LBS receiver and floorplan location application.

## v1.4 fix
This build fixes stale Mosquitto processes left behind by earlier RUCKUS Location Services builds. On startup it identifies only `mosquitto.exe` processes whose command line contains `RUCKUS-Location-Services` and terminates those orphaned application brokers before starting a new broker. The normal Windows Mosquitto service (`mosquitto.exe run`) is never touched.

The application uses a free loopback port in `127.0.0.1:1885-1895` for the local Location Server MQTT client and TCP/8883 for the RUCKUS One TLS-PSK broker.


## v1.7 TLS-PSK compatibility fix

This build explicitly configures Mosquitto for the RUCKUS One TLS 1.2 PSK cipher `ECDHE-PSK-AES256-CBC-SHA384`. This addresses OpenSSL 3.x environments where Mosquitto can otherwise report `no shared cipher` during the RUCKUS One PSK handshake even though TCP/8883 is reachable. The existing Windows Mosquitto service is not stopped or modified; the application launches its own broker instance.


## v1.7 protocol diagnostics

This build follows the RUCKUS technical note more literally for the external MQTT listener: the broker permits MQTT anonymous sessions because RUCKUS One authenticates at the TLS-PSK layer, while retaining the PSK identity/key and explicit ECDHE-PSK-AES256-CBC-SHA384 cipher. Startup no longer opens a localhost test connection to TCP/8883, so broker logs only show real external TLS connection attempts on that port. The receiver records the RUCKUS connection, MQTT CONNACK, SUBSCRIBE, MGQ, MGR, PAQ and PAR stages separately.


## v1.8 broker keeps running across app restarts

RUCKUS One can take many minutes to reconnect to the LBS broker after the connection is dropped. The Mosquitto broker is therefore now started detached from the web app and reused on the next start: restarting only the web app (`stop.bat`, `start.bat`) no longer drops RUCKUS One. The app reattaches to the running broker when the LBS settings (venue name, password, port, bind address) are unchanged, and rebuilds the list of connected clients from the broker log. If the settings change, the old broker is replaced.

* `stop.bat` stops the web app only. Use `stop-broker.bat` to stop this app's broker as well.
* `POST /api/lbs/restart?force=true` replaces the broker even when the settings are unchanged.
* The first restart after upgrading from an older build still drops RUCKUS One once, because the old broker was not detached.

## v1.8 mosquitto.log size

Mosquitto on Windows keeps `mosquitto.log` open without allowing other processes to write to it, so it cannot be truncated or rotated while the broker runs. Rotation is therefore handled by logging far less: the broker no longer logs every message (the `-v` flag and `log_type all` are gone), only connections, subscriptions, warnings and errors. Thousands of PAR messages now add almost nothing to the log. If `mosquitto.log` is over 10 MB when a broker is started fresh, the old content is moved to `mosquitto.log.1` (up to `.5` are kept). Reattaching to a running broker never rotates.

## v1.8 raw log size cap

`logs/lbs_raw.jsonl` (every received MQTT message) is capped at 50 MB. When it reaches the cap it is moved to `lbs_raw.jsonl.1` (older files shift up, `.3` is the oldest kept), so at most about 200 MB is kept. A file that is already over the cap at startup is cut down to its newest 50 MB, kept as `lbs_raw.jsonl.1`, and the oversized original is removed. Binary messages (PAR, MGR) no longer store a second, lossy `payload_utf8` copy of the payload; `payload_base64` has always held the full data. Change `raw_log_max_bytes` / `raw_log_keep` in `lbs_mqtt_server.py` to adjust.
