# Home Assistant Add-on: BMW CarData MQTT Bridge

Bridges BMW CarData MQTT telemetry into Home Assistant using MQTT discovery.

## Features

- BMW OAuth2 device flow
- Explicit GCID setup for BMW CarData stream login
- MQTT v5 BMW broker connection
- Token refresh using the BMW `refresh_token`
- Automatic reconnect/watchdog handling
- MQTT discovery for Home Assistant sensors
- Ingress web UI for setup

## Setup

1. Configure your local MQTT broker options in the add-on configuration.
2. Open the add-on web UI.
3. Enter your BMW CarData `Client-ID`.
4. Enter your BMW CarData stream `GCID` from `Show Connection Details`.
5. Complete the BMW login and select your vehicle(s).
