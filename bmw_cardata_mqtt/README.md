# Home Assistant Add-on: BMW CarData MQTT Bridge

Bridges BMW CarData MQTT telemetry into Home Assistant using MQTT discovery.

## Features

- BMW OAuth2 device flow
- Explicit GCID setup for BMW CarData stream login
- MQTT v5 BMW broker connection
- Token refresh using the BMW `refresh_token`
- Automatic reconnect/watchdog handling
- Automatic fallback from WebSocket transport to plain MQTT/TCP when BMW rejects the WebSocket handshake
- Guarded token refresh retry for auth-like BMW connect errors even if the locally stored token expiry has not elapsed yet
- Periodic REST fallback refresh for the HV battery SoC when the streamed value has gone stale
- MQTT discovery for Home Assistant sensors
- Ingress web UI for setup

## Setup

1. Configure your local MQTT broker options in the add-on configuration.
2. Open the add-on web UI.
3. Enter your BMW CarData `Client-ID`.
4. Enter your BMW CarData stream `GCID` from `Show Connection Details`.
5. Complete the BMW login and select your vehicle(s).

## Troubleshooting

If the add-on logs repeated BMW stream connect failures such as `WebSocket handshake error`, the bridge now:

1. Tries a token refresh once for auth-like connect failures.
2. Falls back from `websockets` to plain `tcp` if the WebSocket handshake is rejected.

In the add-on log, look for lines such as `Attempting token refresh after BMW connect error`, `Tokens refreshed`, or the transport fallback warning to confirm the recovery path was triggered.

If the Home Assistant SoC entity updates too rarely, the bridge now also refreshes `chargingLevelHv` from the BMW vehicle API when the streamed SoC has been stale for a while. Look for log lines such as `Published REST snapshot refresh` to confirm the fallback refresh was used.
