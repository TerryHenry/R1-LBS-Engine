# Integration Notes — v1.2

This release uses the supplied RUCKUS One Consolidated API definition.

Documented endpoints used by the app include:
- POST `/venues/query`
- GET `/venues/{venueId}/floorplans`
- POST `/venues/aps/query`
- POST `/venues/aps/clients/query`
- GET `/venues/{venueId}/floorplans/{floorplanId}/aps/{serialNumber}/floorPositions`
- GET `/venues/{venueId}/signurls/{imageId}/urls`
- POST `/lbsServerProfiles/query`

The LBS profile query exposes profile/server connection status. The raw telemetry transport remains separate and is not inferred from the REST OpenAPI definition.


LBS request timing: v1.2 publishes MGQ/PAQ again immediately after an external RUCKUS One MQTT client is authenticated, then refreshes the request set every 60 seconds for reconnect recovery.


## v1.4 protocol diagnostics
v1.4 does not change the RUCKUS One network endpoint or PSK credentials. It adds diagnostics to distinguish a transport/session problem from an LBS message-format or topic-flow problem.
