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
import re

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
VEHICLE_RAW_FILE = "/data/bmw_vehicles_raw.json"
REFRESH_MARGIN_S = 300                          # refresh 5 min before expiry
STATUS_STABLE_DELAY_S = 5
STREAM_MIN_CONNECT_INTERVAL_S = 60
STREAM_EARLY_DISCONNECT_BACKOFF_S = 5 * 60
QUOTA_ERROR_RESET_S = 4 * 60 * 60
STREAM_FAILURE_RESET_S = 6 * 60 * 60
CIRCUIT_BREAKER_THRESHOLD = 6
CIRCUIT_BREAKER_DELAY_S = 2 * 60 * 60
SLEEP_SLICE_S = 1
BMW_MQTT_KEEPALIVE_S = 30
BMW_MQTT_WS_PATH = "/mqtt"
BMW_MQTT_FALLBACK_DISCONNECT_WINDOW_S = 75


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

    def set(self, **values):
        self.save(values)

    def clear(self):
        self._data = {}
        for p in (self.path, VEHICLE_FILE, VEHICLE_RAW_FILE):
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

    @property
    def next_retry_at(self) -> float:
        return float(self._data.get("next_retry_at", 0) or 0)

    @property
    def last_connected_at(self) -> Optional[str]:
        return self._data.get("last_connected_at")

    @property
    def last_quota_at(self) -> Optional[str]:
        return self._data.get("last_quota_at")

    @property
    def quota_error_count(self) -> int:
        count = int(self._data.get("quota_error_count", 0) or 0)
        if count <= 0:
            return 0
        last_quota_at = self.last_quota_at
        if not last_quota_at:
            return count
        try:
            dt = datetime.fromisoformat(last_quota_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_s = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
            if age_s >= QUOTA_ERROR_RESET_S:
                return 0
        except ValueError:
            return count
        return count

    @property
    def last_stream_connect_attempt_at(self) -> float:
        return float(self._data.get("last_stream_connect_attempt_at", 0) or 0)

    @property
    def last_stream_failure_at(self) -> Optional[str]:
        return self._data.get("last_stream_failure_at")

    @property
    def stream_failure_streak(self) -> int:
        count = int(self._data.get("stream_failure_streak", 0) or 0)
        if count <= 0:
            return 0
        last_failure_at = self.last_stream_failure_at
        if not last_failure_at:
            return count
        try:
            dt = datetime.fromisoformat(last_failure_at)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            age_s = (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()
            if age_s >= STREAM_FAILURE_RESET_S:
                return 0
        except ValueError:
            return count
        return count

    @property
    def next_retry_reason(self) -> str:
        return str(self._data.get("next_retry_reason", "") or "")

    @property
    def request_events(self) -> list[dict]:
        events = self._data.get("request_events", [])
        return events if isinstance(events, list) else []

    @property
    def preferred_stream_transport(self) -> str:
        value = str(self._data.get("preferred_stream_transport", "tcp") or "tcp").lower()
        return value if value in {"tcp", "websockets"} else "tcp"

    @property
    def auth_poll_interval_s(self) -> int:
        return int(self._data.get("auth_poll_interval_s", 0) or 0)

    def record_request(self, endpoint: str, status_code: int | str, method: str = "GET"):
        now = time.time()
        events = [
            event for event in self.request_events
            if isinstance(event, dict) and now - float(event.get("ts", 0) or 0) < 7 * 86400
        ]
        events.append({
            "ts": now,
            "endpoint": endpoint,
            "status": str(status_code),
            "method": method,
        })
        self.save({"request_events": events[-250:]})

    def clear_auth_poll_interval(self):
        self.save({"auth_poll_interval_s": 0})

    def set_auth_poll_interval(self, seconds: int):
        self.save({"auth_poll_interval_s": max(0, int(seconds or 0))})

    def set_preferred_stream_transport(self, transport: str):
        value = str(transport or "tcp").lower()
        if value not in {"tcp", "websockets"}:
            value = "tcp"
        self.save({"preferred_stream_transport": value})

    def record_stream_failure(self) -> int:
        streak = self.stream_failure_streak + 1
        self.save({
            "stream_failure_streak": streak,
            "last_stream_failure_at": datetime.now(timezone.utc).isoformat(),
        })
        return streak

    def clear_stream_failure_streak(self):
        self.save({
            "stream_failure_streak": 0,
            "last_stream_failure_at": None,
        })


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
        self.store.record_request("device_code", resp.status_code, method="POST")
        if resp.status_code != 200:
            raise BMWAuthError(f"Device code request failed {resp.status_code}: {resp.text}")
        data = resp.json()
        user_code = data["user_code"]
        self._device_code = data["device_code"]
        self._interval    = int(data.get("interval", 5))
        self.store.set_auth_poll_interval(self._interval)
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
        self.store.record_request("token_poll", resp.status_code, method="POST")
        if resp.status_code == 200:
            self.store.clear_auth_poll_interval()
            return self._process(resp.json())
        body = resp.json() if "application/json" in resp.headers.get("content-type", "") else {}
        err  = body.get("error", "")
        if err == "slow_down":
            self._interval = min(self._interval + 5, 30)
            self.store.set_auth_poll_interval(self._interval)
            return None
        if err == "authorization_pending":
            return None
        self.store.clear_auth_poll_interval()
        raise BMWAuthError(f"Poll failed {resp.status_code}: {err} – {body.get('error_description','')}")

    @property
    def interval_seconds(self) -> int:
        return max(1, int(self._interval or 5))

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
        store.record_request("token_refresh", resp.status_code, method="POST")
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

def fetch_vehicles(store: BMWTokenStore, force_refresh: bool = False) -> list[dict]:
    """
    GET https://api-cardata.bmwgroup.com/customers/vehicles
    Returns list of {vin, model, brand, year, ...}
    Result is cached to VEHICLE_FILE.
    """
    # Try cache first (valid 24 h)
    if not force_refresh and os.path.exists(VEHICLE_FILE):
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
    store.record_request("vehicles", resp.status_code, method="GET")

    if resp.status_code == 401:
        # Token expired mid-session – try refresh
        if refresh_tokens(store):
            return fetch_vehicles(store)
        raise BMWAuthError("Unauthorized – token refresh failed")

    if resp.status_code == 403:
        log.warning("Vehicle list primary endpoint denied, trying mappings endpoint")
        fallback = requests.get(VEHICLE_MAPPINGS_URL, headers=headers, timeout=15)
        store.record_request("vehicle_mappings", fallback.status_code, method="GET")
        if fallback.status_code == 200:
            raw = fallback.json()
            vehicles = _normalize_vehicle_mappings(raw)
            if vehicles:
                with open(VEHICLE_FILE, "w") as f:
                    json.dump({"_ts": time.time(), "vehicles": vehicles}, f, indent=2)
                with open(VEHICLE_RAW_FILE, "w") as f:
                    json.dump({"_ts": time.time(), "raw": raw}, f, indent=2)
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
    with open(VEHICLE_RAW_FILE, "w") as f:
        json.dump({"_ts": time.time(), "raw": raw}, f, indent=2)

    log.info("Fetched %d vehicle(s) from BMW API", len(vehicles))
    return vehicles


def fetch_vehicle_snapshot(store: BMWTokenStore, vin: str, force_refresh: bool = False) -> dict:
    raw = None
    if not force_refresh and os.path.exists(VEHICLE_RAW_FILE):
        try:
            with open(VEHICLE_RAW_FILE) as f:
                cached = json.load(f)
            if time.time() - cached.get("_ts", 0) < 86400:
                raw = cached.get("raw")
        except Exception:
            raw = None

    if raw is None:
        fetch_vehicles(store, force_refresh=True)
        try:
            with open(VEHICLE_RAW_FILE) as f:
                raw = json.load(f).get("raw")
        except Exception:
            raw = None

    vehicle_payload = _find_vehicle_payload(raw, vin)
    if not vehicle_payload:
        return {}
    return _extract_vehicle_snapshot(vehicle_payload)


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


def _find_vehicle_payload(raw, vin: str) -> Optional[dict]:
    if isinstance(raw, dict):
        if raw.get("vin") == vin:
            return raw
        for key in ("vehicles", "data", "mappings", "items", "results"):
            nested = raw.get(key)
            found = _find_vehicle_payload(nested, vin)
            if found:
                return found
        for value in raw.values():
            found = _find_vehicle_payload(value, vin)
            if found:
                return found
    elif isinstance(raw, list):
        for item in raw:
            found = _find_vehicle_payload(item, vin)
            if found:
                return found
    return None


def _iter_leaf_values(data, path: tuple[str, ...] = ()):
    if isinstance(data, dict):
        for key, value in data.items():
            yield from _iter_leaf_values(value, path + (str(key),))
    elif isinstance(data, list):
        for index, value in enumerate(data):
            yield from _iter_leaf_values(value, path + (str(index),))
    else:
        yield path, data


def _extract_vehicle_snapshot(vehicle_payload: dict) -> dict:
    leaves = list(_iter_leaf_values(vehicle_payload))

    def normalize_path(path: tuple[str, ...]) -> str:
        return re.sub(r"[^a-z0-9]+", "", ".".join(path).lower())

    normalized = [(path, normalize_path(path), value) for path, value in leaves]

    def pick(*aliases: str):
        alias_norms = [re.sub(r"[^a-z0-9]+", "", alias.lower()) for alias in aliases]
        for _path, norm, value in normalized:
            if any(norm == alias or norm.endswith(alias) for alias in alias_norms):
                return value
        return None

    snapshot = {}
    charging_level = pick(
        "chargingLevelHv",
        "stateOfCharge",
        "soc",
        "electricVehicleStateOfCharge",
        "batteryLevel",
        "currentChargeLevel",
    )
    if charging_level is not None:
        snapshot["chargingLevelHv"] = charging_level

    electrical_range = pick(
        "electricRange",
        "remainingElectricRange",
        "kombiRemainingElectricRange",
        "batteryRange",
    )
    if electrical_range is not None:
        snapshot["electricalRange"] = electrical_range

    fuel_level = pick(
        "fuelPercentage",
        "fuelLevel",
        "fuelSystemLevel",
    )
    if fuel_level is not None:
        snapshot["fuelPercentage"] = fuel_level

    mileage = pick(
        "mileage",
        "odometer",
        "travelledDistance",
    )
    if mileage is not None:
        snapshot["mileage"] = mileage

    latitude = pick("latitude", "currentLocationLatitude", "positionLatitude")
    longitude = pick("longitude", "currentLocationLongitude", "positionLongitude")
    if latitude is not None and longitude is not None:
        snapshot["position"] = {
            "latitude": latitude,
            "longitude": longitude,
        }

    return snapshot


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
        {"prop": "lockState",                "name": "Verriegelung",            "icon": "mdi:car-key",             "unit": None,  "dc": None,        "sc": None},
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
        self._next_retry_at = self.store.next_retry_at
        self._next_retry_reason = self.store.next_retry_reason
        self._disconnect_expected = False
        self._stream_transport = self.store.preferred_stream_transport
        self._stream_connected_at_monotonic: Optional[float] = None
        self._stream_message_count_at_connect = 0
        self._dynamic_discovery_keys: set[tuple[str, str]] = set()

        if self._next_retry_at > 0 and self._next_retry_reason not in {"quota_exceeded", "circuit_breaker_open"}:
            log.info(
                "Clearing persisted BMW reconnect delay from previous session (reason=%s)",
                self._next_retry_reason or "unknown",
            )
            self._next_retry_at = 0.0
            self._next_retry_reason = ""
            self.store.set(next_retry_at=0, next_retry_reason="")

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

    @property
    def is_running(self) -> bool:
        return self._running

    def stop(self):
        self._running = False
        self._cancel_offline_timer()
        self._set_status("stopped")
        self._publish_status("offline", connected=False, reason="stopped")
        self._disconnect_clients()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
        self._thread = None

    def _set_status(self, s: str):
        self.status = s
        if self.on_status:
            self.on_status(s)

    # ── Main loop ─────────────────────────────────────────────────────────────

    def _run_loop(self):
        while self._running:
            try:
                self._sleep_until_retry_window()
                if self.store.is_expired:
                    log.info("Token expired, refreshing …")
                    if not refresh_tokens(self.store):
                        self._set_status("auth_error")
                        self._sleep_with_stop(60)
                        continue

                self._sleep_until_stream_connect_allowed()
                self._connect_local()
                self._connect_bmw()
                self._bmw_client.loop_forever(retry_first_connection=False)

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
                self._sleep_with_stop(wait_s)

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
        self._publish_rest_snapshots()
        log.info("Local broker connected, discovery published for %d vehicle(s)", len(self.vehicles))

    # ── BMW MQTT ──────────────────────────────────────────────────────────────

    def _connect_bmw(self):
        gcid     = self.store.gcid
        id_token = self.store.id_token
        mqtt_client_id = self.store.get().get("client_id")
        if not gcid or not id_token:
            raise BMWAuthError("Missing GCID or id_token")
        if not mqtt_client_id:
            raise BMWAuthError("Missing BMW client_id")

        transport = self._stream_transport
        c = mqtt.Client(
            client_id=mqtt_client_id,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
            transport="websockets" if transport == "websockets" else "tcp",
        )
        c.tls_set()
        if transport == "websockets":
            c.ws_set_options(path=BMW_MQTT_WS_PATH)
        c.username_pw_set(gcid, id_token)
        c.on_connect    = self._bmw_on_connect
        c.on_message    = self._bmw_on_message
        c.on_disconnect = self._bmw_on_disconnect
        c.on_subscribe  = self._bmw_on_subscribe
        self.store.set(last_stream_connect_attempt_at=time.time())
        c.connect(BMW_MQTT_HOST, BMW_MQTT_PORT, keepalive=BMW_MQTT_KEEPALIVE_S)
        self._bmw_client = c
        log.info("BMW MQTT connecting as %s with client_id %s via %s …", gcid, mqtt_client_id, transport)

    def _bmw_on_connect(self, client, userdata, flags, rc, props=None):
        reason = self._reason_text(rc)
        if not self._reason_is_success(rc):
            log.error("BMW MQTT connect failed: %s", reason)
            self._set_status("bmw_connect_error")
            self._set_retry_backoff(reason)
            self._publish_status("offline", connected=False, reason=f"connect_failed:{reason}")
            self._disconnect_expected = True
            try:
                client.disconnect()
            except Exception:
                pass
            return
        self._cancel_offline_timer()
        self._clear_retry_backoff()
        self._stream_connected_at_monotonic = time.monotonic()
        self._stream_message_count_at_connect = self.message_count
        self.store.clear_stream_failure_streak()
        self.store.set(
            last_connected_at=datetime.now(timezone.utc).isoformat(),
            quota_error_count=0,
            preferred_stream_transport=self._stream_transport,
        )
        # Subscribe for all selected vehicles
        for v in self.vehicles:
            topic = f"{self.store.gcid}/{v['vin']}"
            client.subscribe(topic, qos=1)
            log.info("Subscribed: %s", topic)
        self._set_status("connected")
        self._publish_status("online", connected=True, reason="connected")

    def _bmw_on_disconnect(self, client, userdata, flags, rc, props=None):
        reason = self._reason_text(rc)
        connected_for_s = None
        if self._stream_connected_at_monotonic is not None:
            connected_for_s = max(0, int(time.monotonic() - self._stream_connected_at_monotonic))
        log.warning(
            "BMW MQTT disconnected: %s%s",
            reason,
            f" after {connected_for_s}s via {self._stream_transport}" if connected_for_s is not None else "",
        )
        if self._disconnect_expected or not self._running:
            self._stream_connected_at_monotonic = None
            self._disconnect_expected = False
            self._schedule_offline_status(reason)
            return
        self._maybe_switch_stream_transport(reason, connected_for_s)
        self._stream_connected_at_monotonic = None
        self._set_status("reconnecting")
        if self._is_early_stream_disconnect(reason, connected_for_s):
            self._set_retry_backoff("Early stream disconnect")
        else:
            self._set_retry_backoff(reason)
        self._schedule_offline_status(reason)

    def _bmw_on_subscribe(self, client, userdata, mid, reason_codes, props=None):
        try:
            reasons = [str(code) for code in (reason_codes or [])]
        except Exception:
            reasons = []
        log.info(
            "BMW MQTT SUBACK mid=%s reasons=%s",
            mid,
            reasons or ["unknown"],
        )

    def _bmw_on_message(self, client, userdata, msg: mqtt.MQTTMessage):
        try:
            payload_str = msg.payload.decode("utf-8", errors="replace")
            self.last_message  = datetime.now(timezone.utc).isoformat()
            self.message_count += 1
            preview = payload_str if len(payload_str) <= 240 else payload_str[:240] + "…"
            log.info("BMW MQTT RX topic=%s bytes=%d payload=%s", msg.topic, len(msg.payload), preview)
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

        def pub(key: str, val, meta: Optional[dict] = None):
            payload = json.dumps({"value": val, "timestamp": ts})
            self._local_client.publish(f"{base}/{key}", payload, retain=True)
            self._publish_dynamic_discovery(vin, key, val, meta or {})

        if isinstance(data, dict):
            items = data.get("data")
            if isinstance(items, dict):
                for prop, item in items.items():
                    val = item.get("value", item) if isinstance(item, dict) else item
                    if prop == "position" and isinstance(val, dict):
                        val = self._normalize_position(val)
                    pub(prop, val, item if isinstance(item, dict) else None)
                    if isinstance(val, dict):
                        for k, v in val.items():
                            pub(f"{prop}/{k}", v)
                return

            if items is None:
                items = [data]
            elif not isinstance(items, list):
                items = [items]

            for item in items:
                if not isinstance(item, dict):
                    pub(event_name, item)
                    continue
                prop = item.get("type") or item.get("name") or event_name
                val  = item.get("value", item)
                if prop == "position" and isinstance(val, dict):
                    val = self._normalize_position(val)
                pub(prop, val, item)
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

    def _publish_rest_snapshots(self):
        for vehicle in self.vehicles:
            vin = vehicle.get("vin")
            if not vin:
                continue
            try:
                snapshot = fetch_vehicle_snapshot(self.store, vin, force_refresh=False)
                if not snapshot:
                    continue
                self._publish_snapshot_values(vin, snapshot)
                log.info("Published REST snapshot fallback for %s with %d field(s)", vin, len(snapshot))
            except Exception as exc:
                log.warning("REST snapshot fallback failed for %s: %s", vin, exc)

    def _publish_snapshot_values(self, vin: str, snapshot: dict):
        ts = datetime.now(timezone.utc).isoformat()
        base = f"{self.prefix}vehicles/{vin}"
        for key, value in snapshot.items():
            payload = json.dumps({
                "value": value,
                "timestamp": ts,
                "source": "rest_snapshot",
            })
            self._local_client.publish(f"{base}/{key}", payload, retain=True)

    def _publish_dynamic_discovery(self, vin: str, prop: str, value, meta: dict):
        if not self._local_client or not prop:
            return

        known_props = {sensor["prop"] for sensor in self.SENSORS}
        if prop in known_props:
            return

        key = (vin, prop)
        if key in self._dynamic_discovery_keys:
            return

        vehicle = next((v for v in self.vehicles if v.get("vin") == vin), None)
        model = vehicle.get("model", "BMW") if vehicle else "BMW"
        safe_prop = self._discovery_safe_id(prop)
        uid = f"bmw_{vin.lower()}_{safe_prop}"
        component = self._dynamic_component_for(prop, value)
        cfg = {
            "name": self._friendly_prop_name(prop),
            "unique_id": uid,
            "state_topic": f"{self.prefix}vehicles/{vin}/{prop}",
            "icon": "mdi:car-info",
            "availability": self._avail(),
            "device": self._device_info(vin, model),
        }

        if component == "binary_sensor":
            cfg["payload_on"] = "ON"
            cfg["payload_off"] = "OFF"
            cfg["value_template"] = "{{ 'ON' if value_json.value else 'OFF' }}"
            device_class = self._dynamic_binary_device_class(prop)
            if device_class:
                cfg["device_class"] = device_class
        else:
            cfg["value_template"] = "{{ value_json.value }}"
            unit = meta.get("unit") if isinstance(meta, dict) else None
            if unit:
                cfg["unit_of_measurement"] = unit
            device_class, state_class = self._dynamic_sensor_classes(prop, value, unit)
            if device_class:
                cfg["device_class"] = device_class
            if state_class:
                cfg["state_class"] = state_class

        self._local_client.publish(
            f"homeassistant/{component}/{uid}/config",
            json.dumps(cfg),
            retain=True,
        )
        self._dynamic_discovery_keys.add(key)

    def _discovery_safe_id(self, value: str) -> str:
        safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", value or "").strip("_").lower()
        return safe or "unknown"

    def _friendly_prop_name(self, prop: str) -> str:
        parts = [part for part in (prop or "").split(".") if part]
        if not parts:
            return "BMW Wert"

        prop_l = prop.lower()

        exact_labels = {
            "vehicle.cabin.door.status": "Tueren verriegelt",
            "vehicle.body.hood.isopen": "Motorhaube offen",
            "vehicle.body.trunk.isopen": "Kofferraum offen",
            "vehicle.body.trunk.door.isopen": "Kofferraumklappe offen",
            "vehicle.cabin.sunroof.status": "Schiebedach Status",
            "vehicle.cabin.sunroof.overallstatus": "Schiebedach Gesamtstatus",
            "vehicle.cabin.sunroof.tiltstatus": "Schiebedach Kippstatus",
            "vehicle.drivetrain.electricengine.charging.status": "Ladestatus",
            "vehicle.drivetrain.electricengine.charging.connectiontype": "Ladeverbindung",
            "vehicle.drivetrain.electricengine.charging.ismsingleimmediatecharging": "Sofortladen aktiv",
            "vehicle.drivetrain.electricengine.charging.issingleimmediatecharging": "Sofortladen aktiv",
            "vehicle.drivetrain.electricengine.kombiremainingelectricrange": "Elektrische Restreichweite",
            "vehicle.drivetrain.lastremainingrange": "Restreichweite",
            "vehicle.drivetrain.fuelsystem.level": "Tankfuellstand",
            "vehicle.drivetrain.fuelsystem.remainingfuel": "Restkraftstoff",
            "vehicle.vehicle.travelleddistance": "Kilometerstand",
            "vehicle.vehicle.avgspeed": "Durchschnittsgeschwindigkeit",
            "vehicle.vehicle.timesetting": "Zeiteinstellung",
            "vehicle.vehicle.preconditioning.activity": "Vorkonditionierung Aktivitaet",
            "vehicle.vehicle.preconditioning.remainingtime": "Vorkonditionierung Restzeit",
            "vehicle.vehicle.preconditioning.error": "Vorkonditionierung Fehler",
            "vehicle.vehicle.preconditioning.isremoteenginerunning": "Fernstart aktiv",
            "vehicle.vehicle.preconditioning.isremoteenginestartallowed": "Fernstart erlaubt",
            "vehicle.powertrain.electri.battery.stateofcharge.target": "Ziel-Akkustand",
            "charginglevelhv": "Akkustand",
            "stateofcharge": "Akkustand",
            "electricalrange": "Elektrische Reichweite",
            "fuelpercentage": "Tankfuellstand",
            "mileage": "Kilometerstand",
            "position": "Standort",
        }
        if prop_l in exact_labels:
            return exact_labels[prop_l]

        door_window_match = re.match(
            r"^vehicle\.cabin\.(door|window)\.(row[12])\.(driver|passenger)\.(isOpen|status)$",
            prop,
            flags=re.IGNORECASE,
        )
        if door_window_match:
            kind, row, side, state = door_window_match.groups()
            kind_label = "Tuer" if kind.lower() == "door" else "Fenster"
            row_label = "vorne" if row.lower() == "row1" else "hinten"
            side_label = "Fahrer" if side.lower() == "driver" else "Beifahrer"
            suffix = "offen" if state.lower() == "isopen" else "Status"
            return f"{kind_label} {side_label} {row_label} {suffix}"

        tire_match = re.match(
            r"^vehicle\.chassis\.axle\.(row[12])\.wheel\.(left|right)\.tire\.(pressure|pressureTarget)$",
            prop,
            flags=re.IGNORECASE,
        )
        if tire_match:
            axle, side, metric = tire_match.groups()
            axle_label = "vorne" if axle.lower() == "row1" else "hinten"
            side_label = "links" if side.lower() == "left" else "rechts"
            metric_label = "Reifendruck Soll" if metric.lower() == "pressuretarget" else "Reifendruck"
            return f"{metric_label} {axle_label} {side_label}"

        location_match = re.match(
            r"^vehicle\.cabin\.infotainment\.navigation\.currentLocation\.(latitude|longitude|heading|altitude)$",
            prop,
            flags=re.IGNORECASE,
        )
        if location_match:
            metric = location_match.group(1).lower()
            label_map = {
                "latitude": "Standort Breitengrad",
                "longitude": "Standort Laengengrad",
                "heading": "Standort Richtung",
                "altitude": "Standort Hoehe",
            }
            return label_map[metric]

        ignored = {"vehicle", "cabin", "body", "drivetrain", "chassis", "electricEngine", "fuelSystem", "infotainment", "navigation", "currentLocation"}
        aliases = {
            "row1": "Vorne",
            "row2": "Hinten",
            "driver": "Fahrer",
            "passenger": "Beifahrer",
            "left": "Links",
            "right": "Rechts",
            "front": "Vorne",
            "rear": "Hinten",
            "isOpen": "Offen",
            "isClosed": "Geschlossen",
            "isRunning": "Aktiv",
            "status": "Status",
            "overallStatus": "Status",
            "tiltStatus": "Kippstatus",
            "pressure": "Druck",
            "pressureTarget": "Zieldruck",
            "remainingTime": "Restzeit",
            "travelledDistance": "Strecke",
            "avgSpeed": "Durchschnittsgeschwindigkeit",
            "lastRemainingRange": "Restreichweite",
            "kombiRemainingElectricRange": "Elektrische Reichweite",
            "chargingLevelHv": "Akkustand",
            "stateOfCharge": "Akkustand",
            "hood": "Motorhaube",
            "trunk": "Kofferraum",
            "charging": "Laden",
            "door": "Tuer",
            "window": "Fenster",
            "sunroof": "Schiebedach",
            "preConditioning": "Vorkonditionierung",
            "timeSetting": "Zeiteinstellung",
        }

        labels: list[str] = []
        for part in parts:
            if part in ignored:
                continue
            token = aliases.get(part)
            if token is None:
                spaced = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", part)
                token = spaced.replace("_", " ").strip()
            if token and token not in labels:
                labels.append(token)

        if not labels:
            labels = [re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", parts[-1]).strip()]

        return " ".join(labels)

    def _dynamic_component_for(self, prop: str, value) -> str:
        if isinstance(value, bool):
            return "binary_sensor"
        if prop.endswith(".isOpen") or prop.endswith(".isClosed") or prop.endswith(".isRunning"):
            return "binary_sensor"
        return "sensor"

    def _dynamic_binary_device_class(self, prop: str) -> Optional[str]:
        prop_l = prop.lower()
        if ".door." in prop_l or prop_l.endswith(".door.isopen"):
            return "door"
        if ".window." in prop_l:
            return "window"
        if ".hood." in prop_l or ".trunk." in prop_l:
            return "opening"
        if ".preconditioning." in prop_l or ".climatization" in prop_l:
            return "running"
        return None

    def _dynamic_sensor_classes(self, prop: str, value, unit: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        prop_l = prop.lower()
        device_class = None
        state_class = None

        if isinstance(value, (int, float)):
            state_class = "measurement"

        if unit == "%":
            device_class = "battery"
        elif unit in {"km", "mi"}:
            device_class = "distance"
        elif unit in {"bar", "kpa"}:
            device_class = "pressure"
        elif unit in {"min", "minutes"}:
            device_class = "duration"
        elif unit in {"km/h", "mph"}:
            device_class = "speed"
        elif prop_l.endswith("latitude") or prop_l.endswith("longitude"):
            device_class = None
        elif prop_l.endswith("heading"):
            device_class = None

        if prop_l.endswith("travelleddistance"):
            state_class = "total_increasing"
            device_class = "distance"

        return device_class, state_class

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
            "next_retry_at": self.store.get().get("next_retry_at"),
            "next_retry_reason": self.store.next_retry_reason,
            "last_connected_at": self.store.last_connected_at,
            "last_quota_at": self.store.last_quota_at,
            "quota_error_count": self.store.quota_error_count,
            "stream_failure_streak": self.store.stream_failure_streak,
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
        failure_streak = self.store.record_stream_failure()
        delay = 30
        retry_reason = "stream_reconnect"
        if "Quota exceeded" in reason:
            retry_reason = "quota_exceeded"
            quota_count = self.store.quota_error_count + 1
            delay = min(15 * 60 * (2 ** max(0, quota_count - 1)), 6 * 60 * 60)
            self.store.set(
                last_quota_at=datetime.now(timezone.utc).isoformat(),
                quota_error_count=quota_count,
            )
        elif "Server busy" in reason:
            retry_reason = "server_busy"
            delay = min(2 * 60 * (2 ** max(0, failure_streak - 1)), 60 * 60)
        elif "Early stream disconnect" in reason:
            retry_reason = "early_stream_disconnect"
            delay = min(
                STREAM_EARLY_DISCONNECT_BACKOFF_S * (2 ** max(0, failure_streak - 1)),
                2 * 60 * 60,
            )
        else:
            delay = min(30 * (2 ** max(0, failure_streak - 1)), 30 * 60)
        if (
            retry_reason in {"stream_reconnect", "early_stream_disconnect", "server_busy"}
            and failure_streak >= CIRCUIT_BREAKER_THRESHOLD
        ):
            retry_reason = "circuit_breaker_open"
            delay = max(delay, CIRCUIT_BREAKER_DELAY_S)
        self._next_retry_at = time.time() + delay
        self._next_retry_reason = retry_reason
        self.store.set(
            next_retry_at=self._next_retry_at,
            next_retry_reason=retry_reason,
        )

    def _clear_retry_backoff(self):
        self._next_retry_at = 0.0
        self._next_retry_reason = ""
        self.store.clear_stream_failure_streak()
        self.store.set(next_retry_at=0, next_retry_reason="")

    def _retry_wait_seconds(self) -> int:
        if self._next_retry_at <= 0:
            return 30
        return max(1, int(self._next_retry_at - time.time()))

    def _sleep_until_retry_window(self):
        wait_s = self._retry_wait_seconds()
        if self._next_retry_at > 0 and wait_s > 1:
            self._set_status("rate_limited")
            self._publish_status("offline", connected=False, reason=self._next_retry_reason or "rate_limited")
            log.info("Waiting %d s before next BMW reconnect attempt", wait_s)
            self._sleep_with_stop(wait_s)

    def _sleep_until_stream_connect_allowed(self):
        last_attempt = self.store.last_stream_connect_attempt_at
        if last_attempt <= 0:
            return
        wait_s = max(0, int(last_attempt + STREAM_MIN_CONNECT_INTERVAL_S - time.time()))
        if wait_s > 0:
            self._set_status("stream_policy_wait")
            self._publish_status(
                "offline",
                connected=False,
                reason=f"stream_connect_policy_wait:{wait_s}s",
            )
            log.info(
                "Waiting %d s to respect BMW stream connection rate policy",
                wait_s,
            )
            self._sleep_with_stop(wait_s)

    def _disconnect_clients(self):
        self._disconnect_expected = True
        self._stream_connected_at_monotonic = None
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

    def _sleep_with_stop(self, seconds: int | float) -> bool:
        remaining = max(0.0, float(seconds or 0))
        while self._running and remaining > 0:
            time.sleep(min(SLEEP_SLICE_S, remaining))
            remaining -= SLEEP_SLICE_S
        return self._running

    def _is_early_stream_disconnect(self, reason: str, connected_for_s: Optional[int]) -> bool:
        if connected_for_s is None:
            return False
        if connected_for_s > BMW_MQTT_FALLBACK_DISCONNECT_WINDOW_S:
            return False
        if self.message_count > self._stream_message_count_at_connect:
            return False
        return "Quota exceeded" not in reason

    def _maybe_switch_stream_transport(self, reason: str, connected_for_s: Optional[int]):
        if connected_for_s is None:
            return
        if connected_for_s > BMW_MQTT_FALLBACK_DISCONNECT_WINDOW_S:
            return
        if self.message_count > self._stream_message_count_at_connect:
            return
        if "Unspecified error" not in reason:
            return
        next_transport = "websockets" if self._stream_transport == "tcp" else "tcp"
        if next_transport == self._stream_transport:
            return
        log.warning(
            "BMW MQTT fallback: switching stream transport from %s to %s after early disconnect",
            self._stream_transport,
            next_transport,
        )
        self._stream_transport = next_transport
        self.store.set_preferred_stream_transport(next_transport)
