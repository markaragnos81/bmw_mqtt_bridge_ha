"""
bmw_client.py – BMW CarData: OAuth2 Device Flow + REST vehicle list + MQTT Bridge

Flow:
  1. Device Code Flow → access_token, refresh_token, id_token, gcid
  2. REST GET /customers/vehicles  → list of {vin, model, ...}
  3. User picks vehicle in UI
  4. Connect BMW MQTT (gcid as user, id_token as password, WebSocket/TLS)
  5. Republish all messages to local broker with split topics
  6. Publish HA MQTT Discovery for all sensors
"""

import base64
import hashlib
import json
import logging
import os
import secrets
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Optional

import requests

log = logging.getLogger("bmw.client")

# ── BMW endpoints ─────────────────────────────────────────────────────────────
BMW_AUTH_BASE    = "https://customer.bmwgroup.com"
DEVICE_CODE_URL  = f"{BMW_AUTH_BASE}/gcdm/oauth/device/code"
TOKEN_URL        = f"{BMW_AUTH_BASE}/gcdm/oauth/token"
VEHICLES_URL     = "https://api-cardata.bmwgroup.com/customers/vehicles"
VEHICLE_MAPPINGS_URL = "https://api-cardata.bmwgroup.com/customers/vehicles/mappings"
BMW_OAUTH_SCOPE  = "authenticate_user openid cardata:api:read cardata:streaming:read"

BMW_MQTT_HOST    = "customer.streaming-cardata.bmwgroup.com"
BMW_MQTT_PORT    = 9000
BMW_ONEID_URL    = "https://customer.bmwgroup.com/oneid/"

TOKEN_FILE       = "/data/bmw_tokens.json"
VEHICLE_FILE     = "/data/bmw_vehicles.json"   # cached vehicle list
REFRESH_MARGIN_S = 300                          # refresh 5 min before expiry
STATUS_STABLE_DELAY_S = 5


class BMWAuthError(Exception):
    pass


# ── Token store ───────────────────────────────────────────────────────────────

class BMWTokenStore:
    def __init__(self, path: str = TOKEN_FILE):
        self.path  = path
        self._data: dict = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path) as f:
                    self._data = json.load(f)
            except Exception:
                self._data = {}

    def save(self, tokens: dict):
        self._data = {**self._data, **tokens}
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(self.path, "w") as f:
            json.dump(self._data, f, indent=2)

    def get(self) -> dict:
        return self._data

    def clear(self):
        self._data = {}
        for p in (self.path, VEHICLE_FILE):
            if os.path.exists(p):
                os.remove(p)

    # ── properties ────────────────────────────────────────────────────────────
    @property
    def has_tokens(self) -> bool:
        return bool(self._data.get("refresh_token"))

    @property
    def gcid(self) -> Optional[str]:
        return self._data.get("gcid")

    @property
    def access_token(self) -> Optional[str]:
        return self._data.get("access_token")

    @property
    def id_token(self) -> Optional[str]:
        return self._data.get("id_token")

    @property
    def refresh_token(self) -> Optional[str]:
        return self._data.get("refresh_token")

    @property
    def expires_at(self) -> Optional[float]:
        return self._data.get("expires_at")

    @property
    def is_expired(self) -> bool:
        if not self.expires_at:
            return True
        return time.time() >= (self.expires_at - REFRESH_MARGIN_S)

    @property
    def selected_vins(self) -> list[str]:
        return self._data.get("selected_vins", [])

    def set_selected_vins(self, vins: list[str]):
        self._data["selected_vins"] = vins
        self.save({})


# ── OAuth2 Device Flow ────────────────────────────────────────────────────────

class BMWDeviceFlow:
    def __init__(self, client_id: str, store: BMWTokenStore, gcid: Optional[str] = None):
        self.client_id    = client_id
        self.store        = store
        self.gcid         = gcid
        self._device_code: Optional[str] = None
        self._code_verifier: Optional[str] = None
        self._interval    = 5

    def start(self) -> dict:
        """Start flow → {user_code, verification_uri, expires_in}."""
        self._code_verifier = _generate_code_verifier()
        code_challenge = _code_challenge(self._code_verifier)
        resp = requests.post(
            DEVICE_CODE_URL,
            data={
                "client_id": self.client_id,
                "scope": BMW_OAUTH_SCOPE,
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
            },
            timeout=15,
        )
        if resp.status_code != 200:
            raise BMWAuthError(f"Device code request failed {resp.status_code}: {resp.text}")
        data = resp.json()
        user_code = data["user_code"]
        self._device_code = data["device_code"]
        self._interval    = int(data.get("interval", 5))
        verification_uri = _sanitize_verification_uri(
            data.get("verification_uri"),
            user_code=user_code,
        )
        verification_uri_complete = _sanitize_verification_uri(
            data.get("verification_uri_complete"),
            user_code=user_code,
        )
        return {
            "user_code":                 user_code,
            "verification_uri":          verification_uri,
            "verification_uri_complete": verification_uri_complete or verification_uri,
            "verification_host":         _display_verification_host(verification_uri_complete or verification_uri),
            "expires_in":                int(data.get("expires_in", 300)),
        }

    def poll(self) -> Optional[dict]:
        """Poll once. Returns token dict on success, None if pending."""
        if not self._device_code:
            raise BMWAuthError("Device flow not started")
        resp = requests.post(
            TOKEN_URL,
            data={
                "client_id":     self.client_id,
                "device_code":   self._device_code,
                "grant_type":    "urn:ietf:params:oauth:grant-type:device_code",
                "code_verifier": self._code_verifier,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            return self._process(resp.json())
        body = resp.json() if "application/json" in resp.headers.get("content-type", "") else {}
        err  = body.get("error", "")
        if err in ("authorization_pending", "slow_down"):
            return None
        raise BMWAuthError(f"Poll failed {resp.status_code}: {err} – {body.get('error_description','')}")

    def _process(self, data: dict) -> dict:
        gcid   = self.gcid or _extract_gcid(data.get("id_token") or data["access_token"])
        tokens = {
            "access_token":  data["access_token"],
            "refresh_token": data["refresh_token"],
            "id_token":      data.get("id_token", data["access_token"]),
            "expires_at":    time.time() + int(data.get("expires_in", 3600)),
            "gcid":          gcid,
            "client_id":     self.client_id,
        }
        self.store.save(tokens)
        return tokens


def _extract_gcid(jwt: str) -> Optional[str]:
    try:
        pad     = jwt.split(".")[1]
        pad    += "=" * (4 - len(pad) % 4)
        payload = json.loads(base64.urlsafe_b64decode(pad))
        return payload.get("gcid") or payload.get("sub")
    except Exception:
        return None


def _sanitize_verification_uri(value: Optional[str], user_code: Optional[str] = None) -> str:
    if not value or not isinstance(value, str):
        return _oneid_with_code(user_code)
    text = value.strip()
    if not text:
        return _oneid_with_code(user_code)
    if text.lower().startswith("<svg"):
        return _oneid_with_code(user_code)
    if text.startswith("http://") or text.startswith("https://"):
        if "customer.bmwgroup.com/oneid" in text and user_code and "user_code=" not in text:
            sep = "&" if "?" in text else "?"
            return f"{text}{sep}user_code={user_code}"
        return text
    return _oneid_with_code(user_code)


def _oneid_with_code(user_code: Optional[str]) -> str:
    if user_code:
        return f"{BMW_ONEID_URL}?user_code={user_code}"
    return BMW_ONEID_URL


def _display_verification_host(value: str) -> str:
    if "customer.bmwgroup.com/oneid" in value:
        return "customer.bmwgroup.com/oneid"
    return value


def _generate_code_verifier(length: int = 96) -> str:
    verifier = secrets.token_urlsafe(length)
    return verifier[:length]


def _code_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def refresh_tokens(store: BMWTokenStore) -> bool:
    client_id = store.get().get("client_id")
    if not client_id or not store.refresh_token:
        return False
    try:
        resp = requests.post(
            TOKEN_URL,
            data={
                "client_id":     client_id,
                "refresh_token": store.refresh_token,
                "grant_type":    "refresh_token",
            },
            timeout=15,
        )
        if resp.status_code == 200:
            data   = resp.json()
            update = {
                "access_token": data["access_token"],
                "id_token":     data.get("id_token", data["access_token"]),
                "expires_at":   time.time() + int(data.get("expires_in", 3600)),
            }
            if "refresh_token" in data:
                update["refresh_token"] = data["refresh_token"]
            store.save(update)
            log.info("Tokens refreshed")
            return True
        log.warning("Token refresh %s: %s", resp.status_code, resp.text)
        return False
    except Exception as exc:
        log.error("Token refresh error: %s", exc)
        return False


# ── Vehicle list API ──────────────────────────────────────────────────────────

def fetch_vehicles(store: BMWTokenStore) -> list[dict]:
    """
    GET https://api-cardata.bmwgroup.com/customers/vehicles
    Returns list of {vin, model, brand, year, ...}
    Result is cached to VEHICLE_FILE.
    """
    # Try cache first (valid 24 h)
    if os.path.exists(VEHICLE_FILE):
        try:
            with open(VEHICLE_FILE) as f:
                cached = json.load(f)
            if time.time() - cached.get("_ts", 0) < 86400:
                log.info("Using cached vehicle list (%d vehicles)", len(cached.get("vehicles", [])))
                return cached["vehicles"]
        except Exception:
            pass

    if not store.access_token:
        raise BMWAuthError("No access token available")

    headers = {
        "Authorization": f"Bearer {store.access_token}",
        "x-version":     "v1",
        "Accept":        "application/json",
    }
    resp = requests.get(VEHICLES_URL, headers=headers, timeout=15)

    if resp.status_code == 401:
        # Token expired mid-session – try refresh
        if refresh_tokens(store):
            return fetch_vehicles(store)
        raise BMWAuthError("Unauthorized – token refresh failed")

    if resp.status_code == 403:
        log.warning("Vehicle list primary endpoint denied, trying mappings endpoint")
        fallback = requests.get(VEHICLE_MAPPINGS_URL, headers=headers, timeout=15)
        if fallback.status_code == 200:
            raw = fallback.json()
            vehicles = _normalize_vehicle_mappings(raw)
            if vehicles:
                with open(VEHICLE_FILE, "w") as f:
                    json.dump({"_ts": time.time(), "vehicles": vehicles}, f, indent=2)
                log.info("Fetched %d vehicle(s) from BMW mappings API", len(vehicles))
                return vehicles
        raise BMWAuthError(
            "Vehicle list access forbidden. Your BMW CarData client likely needs "
            "'cardata:api:read' in addition to 'cardata:streaming:read'. "
            f"Primary endpoint: {resp.status_code}; fallback: {fallback.status_code}."
        )

    if resp.status_code != 200:
        raise BMWAuthError(f"Vehicle list failed {resp.status_code}: {resp.text}")

    raw      = resp.json()
    vehicles = _normalize_vehicles(raw)

    with open(VEHICLE_FILE, "w") as f:
        json.dump({"_ts": time.time(), "vehicles": vehicles}, f, indent=2)

    log.info("Fetched %d vehicle(s) from BMW API", len(vehicles))
    return vehicles


def _normalize_vehicles(raw) -> list[dict]:
    """Normalize BMW API vehicle response to a flat list."""
    # BMW returns either a list directly or {"vehicles": [...]}
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = raw.get("vehicles") or raw.get("data") or [raw]
    else:
        return []

    result = []
    for v in items:
        if not isinstance(v, dict):
            continue
        vin   = v.get("vin") or v.get("id", "")
        if not vin or len(vin) != 17:
            continue
        model = (
            v.get("modelName") or v.get("model") or
            v.get("brand", "BMW") + " " + v.get("series", "")
        ).strip() or "BMW"
        result.append({
            "vin":   vin,
            "model": model,
            "brand": v.get("brand", "BMW"),
            "year":  v.get("modelYear") or v.get("year", ""),
            "color": v.get("color", ""),
        })
    return result


def _normalize_vehicle_mappings(raw) -> list[dict]:
    if isinstance(raw, list):
        items = raw
    elif isinstance(raw, dict):
        items = raw.get("vehicles") or raw.get("data") or raw.get("mappings") or [raw]
    else:
        return []

    result = []
    for item in items:
        if not isinstance(item, dict):
            continue
        vin = (
            item.get("vin")
            or item.get("vehicleVin")
            or item.get("customerVehicleId")
            or item.get("vehicleId")
            or item.get("id", "")
        )
        if not vin or len(vin) != 17:
            continue
        model = (
            item.get("modelName")
            or item.get("model")
            or item.get("brandModel")
            or item.get("name")
            or "BMW"
        )
        result.append({
            "vin": vin,
            "model": model,
            "brand": item.get("brand", "BMW"),
            "year": item.get("modelYear") or item.get("year", ""),
            "color": item.get("color", ""),
        })
    return result


# ── MQTT Bridge ───────────────────────────────────────────────────────────────

import paho.mqtt.client as mqtt


class BMWMQTTBridge:
    SENSORS = [
        {"prop": "fuelPercentage",          "name": "Kraftstoff",              "icon": "mdi:gas-station",         "unit": "%",   "dc": "battery",   "sc": "measurement"},
        {"prop": "chargingLevelHv",          "name": "Ladestand",               "icon": "mdi:battery-charging",    "unit": "%",   "dc": "battery",   "sc": "measurement"},
        {"prop": "range_km",                 "name": "Reichweite",              "icon": "mdi:map-marker-distance", "unit": "km",  "dc": "distance",  "sc": "measurement"},
        {"prop": "electricalRange",          "name": "Elektr. Reichweite",      "icon": "mdi:lightning-bolt",      "unit": "km",  "dc": "distance",  "sc": "measurement"},
        {"prop": "fuelRange",                "name": "Kraftstoff-Reichweite",   "icon": "mdi:gas-station",         "unit": "km",  "dc": "distance",  "sc": "measurement"},
        {"prop": "chargingStatus",           "name": "Ladestatus",              "icon": "mdi:ev-plug-type2",       "unit": None,  "dc": None,        "sc": None},
        {"prop": "chargingConnectionType",   "name": "Ladeanschluss",           "icon": "mdi:power-plug",          "unit": None,  "dc": None,        "sc": None},
        {"prop": "remainingChargingMinutes", "name": "Restladezeit",            "icon": "mdi:timer-outline",       "unit": "min", "dc": "duration",  "sc": "measurement"},
        {"prop": "chargingTargetSoc",        "name": "Ladeziel",                "icon": "mdi:battery-charging-100","unit": "%",   "dc": "battery",   "sc": "measurement"},
        {"prop": "mileage",                  "name": "Kilometerstand",          "icon": "mdi:counter",             "unit": "km",  "dc": "distance",  "sc": "total_increasing"},
        {"prop": "doorsState",               "name": "Türen",                   "icon": "mdi:car-door",            "unit": None,  "dc": None,        "sc": None},
        {"prop": "windowsState",             "name": "Fenster",                 "icon": "mdi:car-select",          "unit": None,  "dc": None,        "sc": None},
        {"prop": "lockState",                "name": "Verriegelung",            "icon": "mdi:car-key",             "unit": None,  "dc": "lock",      "sc": None},
        {"prop": "hood",                     "name": "Motorhaube",              "icon": "mdi:car",                 "unit": None,  "dc": None,        "sc": None},
        {"prop": "trunk",                    "name": "Kofferraum",              "icon": "mdi:car-back",            "unit": None,  "dc": None,        "sc": None},
        {"prop": "climateActive",            "name": "Klimaanlage aktiv",       "icon": "mdi:air-conditioner",     "unit": None,  "dc": None,        "sc": None},
        {"prop": "tirePressureFrontLeft",    "name": "Reifendruck VL",          "icon": "mdi:tire",                "unit": "bar", "dc": "pressure",  "sc": "measurement"},
        {"prop": "tirePressureFrontRight",   "name": "Reifendruck VR",          "icon": "mdi:tire",                "unit": "bar", "dc": "pressure",  "sc": "measurement"},
        {"prop": "tirePressureRearLeft",     "name": "Reifendruck HL",          "icon": "mdi:tire",                "unit": "bar", "dc": "pressure",  "sc": "measurement"},
        {"prop": "tirePressureRearRight",    "name": "Reifendruck HR",          "icon": "mdi:tire",                "unit": "bar", "dc": "pressure",  "sc": "measurement"},
        {"prop": "lastFetchedAt",            "name": "Letzte Aktualisierung",   "icon": "mdi:clock-outline",       "unit": None,  "dc": "timestamp", "sc": None},
    ]

    def __init__(
        self,
        store: BMWTokenStore,
        vehicles: list[dict],          # [{vin, model, ...}]
        local_host: str,
        local_port: int,
        local_user: str,
        local_password: str,
        local_prefix: str,
        on_status_change: Optional[Callable[[str], None]] = None,
    ):
        self.store        = store
        self.vehicles     = vehicles   # list of vehicle dicts
        self.local_host   = local_host
        self.local_port   = local_port
        self.local_user   = local_user
        self.local_password = local_password
        self.prefix       = local_prefix.rstrip("/") + "/"
        self.on_status    = on_status_change

        self._bmw_client:   Optional[mqtt.Client] = None
        self._local_client: Optional[mqtt.Client] = None
        self._running     = False
        self._thread:     Optional[threading.Thread] = None
        self._offline_timer: Optional[threading.Timer] = None
        self._next_retry_at = 0.0

        self.status        = "stopped"
        self.last_message  = None
        self.message_count = 0
        self.last_status_payload: Optional[dict] = None

    # ── Control ───────────────────────────────────────────────────────────────

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread  = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._cancel_offline_timer()
        self._set_status("stopped")
        self._publish_status("offline", connected=False, reason="stopped")
        for c in (self._bmw_client, self._local_client):
            try:
                if c:
                    c.disconnect()
            except Exception:
                pass

    def _set_status(self, s: str):
        self.status = s
        if self.on_status:
            self.on_status(s)

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _run_loop(self):
        while self._running:
            try:
                if self.store.is_expired:
                    log.info("Token expired, refreshing …")
                    if not refresh_tokens(self.store):
                        self._set_status("auth_error")
                        time.sleep(60)
                        continue

                self._connect_local()
                self._connect_bmw()
                self._bmw_client.loop_forever(retry_first_connection=True)

            except Exception as exc:
                log.error("Bridge loop: %s", exc)
                self._set_status("error")
                self._publish_status("offline", connected=False, reason=f"bridge_error:{exc}")
                if "Quota exceeded" in str(exc):
                    self._set_retry_backoff("Quota exceeded")

            if self._running:
                self._disconnect_clients()
                wait_s = self._retry_wait_seconds()
                log.info("Reconnecting in %d s …", wait_s)
                time.sleep(wait_s)

    # ── Local MQTT ────────────────────────────────────────────────────────────

    def _connect_local(self):
        c = mqtt.Client(
            client_id="bmw-ha-bridge-local",
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
        )
        if self.local_user:
            c.username_pw_set(self.local_user, self.local_password)
        c.connect(self.local_host, self.local_port, keepalive=60)
        c.loop_start()
        self._local_client = c
        self._publish_status("starting", connected=False, reason="local_connected")
        # Publish discovery for all selected vehicles
        for v in self.vehicles:
            self._publish_discovery(v["vin"], v.get("model", "BMW"))
        log.info("Local broker connected, discovery published for %d vehicle(s)", len(self.vehicles))

    # ── BMW MQTT ──────────────────────────────────────────────────────────────

    def _connect_bmw(self):
        gcid     = self.store.gcid
        id_token = self.store.id_token
        if not gcid or not id_token:
            raise BMWAuthError("Missing GCID or id_token")

        c = mqtt.Client(
            client_id=gcid,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
        )
        c.tls_set()
        c.username_pw_set(gcid, id_token)
        c.on_connect    = self._bmw_on_connect
        c.on_message    = self._bmw_on_message
        c.on_disconnect = self._bmw_on_disconnect
        c.connect(BMW_MQTT_HOST, BMW_MQTT_PORT, keepalive=60)
        self._bmw_client = c
        log.info("BMW MQTT connecting as %s …", gcid)

    def _bmw_on_connect(self, client, userdata, flags, rc, props=None):
        reason = self._reason_text(rc)
        if not self._reason_is_success(rc):
            log.error("BMW MQTT connect failed: %s", reason)
            self._set_status("bmw_connect_error")
            self._set_retry_backoff(reason)
            self._publish_status("offline", connected=False, reason=f"connect_failed:{reason}")
            return
        self._cancel_offline_timer()
        self._clear_retry_backoff()
        # Subscribe for all selected vehicles
        for v in self.vehicles:
            topic = f"{self.store.gcid}/{v['vin']}/#"
            client.subscribe(topic, qos=1)
            log.info("Subscribed: %s", topic)
        self._set_status("connected")
        self._publish_status("online", connected=True, reason="connected")

    def _bmw_on_disconnect(self, client, userdata, flags, rc, props=None):
        reason = self._reason_text(rc)
        log.warning("BMW MQTT disconnected: %s", reason)
        self._set_status("reconnecting")
        self._set_retry_backoff(reason)
        self._schedule_offline_status(reason)

    def _bmw_on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        try:
            payload_str = msg.payload.decode("utf-8", errors="replace")
            self.last_message  = datetime.now(timezone.utc).isoformat()
            self.message_count += 1
            if self.status != "connected":
                self._set_status("connected")
            self._publish_status("online", connected=True, reason="message_received")

            # topic: gcid/VIN/eventName  or  gcid/VIN
            parts      = msg.topic.split("/")
            vin        = parts[1] if len(parts) >= 2 else "unknown"
            event_name = parts[2] if len(parts) >= 3 else "data"

            # Raw topic
            self._local_client.publish(
                f"{self.prefix}raw/{vin}/{event_name}", payload_str, retain=True
            )

            # Split topics
            try:
                self._publish_split(vin, event_name, json.loads(payload_str))
            except json.JSONDecodeError:
                pass

        except Exception as exc:
            log.error("Message error: %s", exc)

    def _publish_split(self, vin: str, event_name: str, data):
        base = f"{self.prefix}vehicles/{vin}"
        ts   = self.last_message

        def pub(key: str, val):
            payload = json.dumps({"value": val, "timestamp": ts})
            self._local_client.publish(f"{base}/{key}", payload, retain=True)

        if isinstance(data, dict):
            items = data.get("data", [data])
            if not isinstance(items, list):
                items = [data]
            for item in items:
                if not isinstance(item, dict):
                    pub(event_name, item)
                    continue
                prop = item.get("type") or item.get("name") or event_name
                val  = item.get("value", item)
                if prop == "position" and isinstance(val, dict):
                    val = self._normalize_position(val)
                pub(prop, val)
                # Also emit sub-keys for HA template convenience
                if isinstance(val, dict):
                    for k, v in val.items():
                        pub(f"{prop}/{k}", v)
        else:
            pub(event_name, data)

    # ── MQTT Discovery ────────────────────────────────────────────────────────

    def _device_info(self, vin: str, model: str) -> dict:
        return {
            "identifiers":   [f"bmw_{vin}"],
            "name":          model,
            "manufacturer":  "BMW",
            "model":         model,
            "serial_number": vin,
        }

    def _avail(self) -> list:
        return [{
            "topic":                 f"{self.prefix}status/availability",
            "payload_available":     "online",
            "payload_not_available": "offline",
        }]

    def _publish_discovery(self, vin: str, model: str):
        for s in self.SENSORS:
            uid = f"bmw_{vin.lower()}_{s['prop']}"
            cfg: dict = {
                "name":           f"{model} {s['name']}",
                "unique_id":      uid,
                "state_topic":    f"{self.prefix}vehicles/{vin}/{s['prop']}",
                "value_template": "{{ value_json.value }}",
                "icon":           s["icon"],
                "availability":   self._avail(),
                "device":         self._device_info(vin, model),
            }
            if s["unit"]: cfg["unit_of_measurement"] = s["unit"]
            if s["dc"]:   cfg["device_class"]         = s["dc"]
            if s["sc"]:   cfg["state_class"]           = s["sc"]
            self._local_client.publish(
                f"homeassistant/sensor/{uid}/config", json.dumps(cfg), retain=True
            )

        # Device Tracker
        uid = f"bmw_{vin.lower()}_location"
        self._local_client.publish(
            f"homeassistant/device_tracker/{uid}/config",
            json.dumps({
                "name":                     f"{model} Position",
                "unique_id":                uid,
                "icon":                     "mdi:car-connected",
                "json_attributes_topic":    f"{self.prefix}vehicles/{vin}/position",
                "json_attributes_template": "{{ value_json.value | tojson }}",
                "source_type":              "gps",
                "availability":             self._avail(),
                "device":                   self._device_info(vin, model),
            }), retain=True
        )
        log.info("Discovery published: %s (%s)", vin, model)

    def _normalize_position(self, payload: dict) -> dict:
        latitude = payload.get("latitude", payload.get("lat"))
        longitude = payload.get("longitude", payload.get("lon"))
        gps_accuracy = payload.get("gps_accuracy", payload.get("accuracy"))
        normalized = dict(payload)
        if latitude is not None:
            normalized["latitude"] = latitude
        if longitude is not None:
            normalized["longitude"] = longitude
        if gps_accuracy is not None:
            normalized["gps_accuracy"] = gps_accuracy
        return normalized

    def _publish_status(self, state: str, connected: bool, reason: str):
        if not self._local_client:
            return
        payload = {
            "state": state,
            "connected": connected,
            "reason": reason,
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "last_message": self.last_message,
            "message_count": self.message_count,
        }
        if self.store.expires_at:
            payload["token_expires_at"] = datetime.fromtimestamp(
                self.store.expires_at, tz=timezone.utc
            ).isoformat()
        self.last_status_payload = payload
        self._local_client.publish(
            f"{self.prefix}status",
            json.dumps(payload),
            retain=True,
        )
        self._local_client.publish(
            f"{self.prefix}status/availability",
            "online" if connected else "offline",
            retain=True,
        )

    def _schedule_offline_status(self, reason: str):
        self._cancel_offline_timer()
        self._offline_timer = threading.Timer(
            STATUS_STABLE_DELAY_S,
            self._publish_offline_if_still_disconnected,
            kwargs={"reason": reason},
        )
        self._offline_timer.daemon = True
        self._offline_timer.start()

    def _publish_offline_if_still_disconnected(self, reason: str):
        self._offline_timer = None
        if not self._running or self.status != "connected":
            self._publish_status("offline", connected=False, reason=reason)

    def _cancel_offline_timer(self):
        if self._offline_timer:
            self._offline_timer.cancel()
            self._offline_timer = None

    def _set_retry_backoff(self, reason: str):
        delay = 30
        if "Quota exceeded" in reason:
            delay = 15 * 60
        elif "Server busy" in reason:
            delay = 2 * 60
        self._next_retry_at = time.time() + delay

    def _clear_retry_backoff(self):
        self._next_retry_at = 0.0

    def _retry_wait_seconds(self) -> int:
        if self._next_retry_at <= 0:
            return 30
        return max(1, int(self._next_retry_at - time.time()))

    def _disconnect_clients(self):
        for client in (self._bmw_client, self._local_client):
            if not client:
                continue
            try:
                client.disconnect()
            except Exception:
                pass
            try:
                client.loop_stop()
            except Exception:
                pass
        self._bmw_client = None
        self._local_client = None

    def _reason_is_success(self, reason_code) -> bool:
        if reason_code is None:
            return True
        if isinstance(reason_code, int):
            return reason_code == 0
        return not getattr(reason_code, "is_failure", False)

    def _reason_text(self, reason_code) -> str:
        if reason_code is None:
            return "success"
        return str(reason_code)
