"""
webapp.py – BMW CarData HA Addon: Ingress Web UI

Phasen:
  mqtt_check  → MQTT-Verbindung testen (Konfiguration aus options.json)
  setup       → Client-ID eingeben
  auth        → BMW Device Flow (warte auf Browser-Login)
  pick        → Fahrzeug(e) auswählen aus BMW-Fahrzeugliste
  dashboard   → Live-Status
"""

import json
import logging
import os
import queue
import re
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from flask import Flask, Response, jsonify, redirect, render_template_string, request, session

from bmw_client import (
    BMWAuthError, BMWDeviceFlow, BMWMQTTBridge, BMWTokenStore,
    fetch_vehicles, refresh_tokens,
)

log = logging.getLogger("bmw.webapp")

# ── Global state ──────────────────────────────────────────────────────────────
store:  BMWTokenStore        = BMWTokenStore()
bridge: Optional[BMWMQTTBridge] = None
_sse_queues: list[queue.Queue] = []
INGRESS_PATH = os.getenv("INGRESS_PATH", "")

OPTIONS_FILE = "/data/options.json"
OVERRIDE_FILE = "/data/bmw_setup.json"  # Client-ID + selected VINs


def load_options() -> dict:
    if os.path.exists(OPTIONS_FILE):
        with open(OPTIONS_FILE) as f:
            data = json.load(f)
            if "stream_mode" not in data:
                data["stream_mode"] = "always_on"
            if "active_window" not in data:
                data["active_window"] = ""
            return data
    return {"stream_mode": "always_on", "active_window": ""}


def load_override() -> dict:
    if os.path.exists(OVERRIDE_FILE):
        with open(OVERRIDE_FILE) as f:
            return json.load(f)
    return {}


def save_override(data: dict):
    cur = load_override()
    cur.update(data)
    with open(OVERRIDE_FILE, "w") as f:
        json.dump(cur, f, indent=2)


def stream_mode() -> str:
    return load_options().get("stream_mode", "always_on")


def active_window() -> str:
    return load_options().get("active_window", "").strip()


def _parse_hhmm(value: str) -> int:
    hour, minute = value.split(":", 1)
    return int(hour) * 60 + int(minute)


def _window_ranges(raw: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for chunk in [part.strip() for part in raw.split(",") if part.strip()]:
        if not re.match(r"^\d{2}:\d{2}-\d{2}:\d{2}$", chunk):
            continue
        start_s, end_s = chunk.split("-", 1)
        ranges.append((_parse_hhmm(start_s), _parse_hhmm(end_s)))
    return ranges


def within_active_window(now_ts: Optional[float] = None) -> bool:
    raw = active_window()
    if not raw:
        return True
    now = datetime.fromtimestamp(now_ts or time.time())
    cur = now.hour * 60 + now.minute
    for start, end in _window_ranges(raw):
        if start == end:
            return True
        if start < end and start <= cur < end:
            return True
        if start > end and (cur >= start or cur < end):
            return True
    return False


def next_window_start(now_ts: Optional[float] = None) -> Optional[str]:
    raw = active_window()
    if not raw:
        return None
    now = datetime.fromtimestamp(now_ts or time.time())
    cur = now.hour * 60 + now.minute
    candidates = []
    for start, _end in _window_ranges(raw):
        delta = (start - cur) % (24 * 60)
        candidates.append(delta)
    if not candidates:
        return None
    delta_min = min(candidates)
    target = now.timestamp() + delta_min * 60
    return datetime.fromtimestamp(target).strftime("%d.%m %H:%M")


def _format_countdown(target_ts: Optional[float]) -> Optional[str]:
    if not target_ts or target_ts <= time.time():
        return None
    seconds = max(1, int(target_ts - time.time()))
    minutes, _seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"noch {hours}h {minutes:02d}m"
    return f"noch {minutes} min"


def _local_tz():
    return datetime.now().astimezone().tzinfo


def _format_local_timestamp(ts: Optional[float], with_tz: bool = True) -> Optional[str]:
    if not ts:
        return None
    fmt = "%d.%m %H:%M %Z" if with_tz else "%d.%m %H:%M"
    return datetime.fromtimestamp(ts, tz=_local_tz()).strftime(fmt)


def _format_local_iso(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return value
    return dt.astimezone(_local_tz()).strftime("%d.%m %H:%M %Z")


def _retry_ui_state() -> dict:
    retry_ts = store.next_retry_at
    retry_str = _format_local_timestamp(retry_ts)
    retry_countdown = _format_countdown(retry_ts)
    retry_reason = store.next_retry_reason
    retry_hint = None
    if retry_str and retry_countdown and retry_reason == "quota_exceeded":
        retry_hint = f"Rate-Limit aktiv bis {retry_str} ({retry_countdown})"
    reconnect_hint = None
    if retry_str and retry_countdown and retry_reason and retry_reason != "quota_exceeded":
        reconnect_hint = f"BMW-Stream Reconnect ab {retry_str} ({retry_countdown})"
    return {
        "next_retry_at": retry_str,
        "retry_countdown": retry_countdown,
        "retry_hint": retry_hint,
        "reconnect_hint": reconnect_hint,
        "retry_reason": retry_reason,
    }


def _block_reason_state() -> dict:
    window_open = within_active_window()
    retry_state = _retry_ui_state()
    quota_blocked = bool(retry_state["retry_hint"])
    reasons = []
    if not window_open:
        reasons.append("Auto-Start pausiert")
    if quota_blocked:
        reasons.append("BMW Rate-Limit aktiv")
    blocked = bool(reasons)
    block_reason = " + ".join(reasons) if reasons else None
    block_hint = None
    if blocked:
        detail_parts = []
        if not window_open and active_window():
            detail_parts.append(f"Auto-Start-Fenster: {active_window()}")
            if next_window_start():
                detail_parts.append(f"nächstes Fenster ab {next_window_start()}")
        if quota_blocked:
            detail_parts.append(retry_state["retry_hint"])
        elif retry_state["reconnect_hint"]:
            detail_parts.append(retry_state["reconnect_hint"])
        block_hint = "Hinweis: " + " · ".join(detail_parts)
    return {
        "blocked": blocked,
        "block_reason": block_reason,
        "block_hint": block_hint,
    }


def _request_telemetry() -> dict:
    now = time.time()
    events = [
        event for event in store.request_events
        if isinstance(event, dict)
    ]
    last_24h = [
        event for event in events
        if now - float(event.get("ts", 0) or 0) <= 86400
    ]
    last_request = last_24h[-1] if last_24h else (events[-1] if events else None)
    last_request_at = None
    last_request_summary = None
    if last_request:
        ts = float(last_request.get("ts", 0) or 0)
        last_request_at = _format_local_timestamp(ts)
        last_request_summary = f"{last_request.get('method', 'GET')} {last_request.get('endpoint', '?')} -> {last_request.get('status', '?')}"

    interval_hint = "Kein periodisches HTTP-Polling aktiv"
    auth_poll_interval_s = store.auth_poll_interval_s
    if auth_poll_interval_s > 0:
        interval_hint = f"BMW OneID Polling aktuell alle {auth_poll_interval_s}s"
    elif store.next_retry_at > time.time():
        interval_hint = "Kein HTTP-Polling; BMW MQTT Reconnect läuft im Backoff"
    recent_spacing = None
    if len(last_24h) >= 2:
        ts_values = [float(event.get("ts", 0) or 0) for event in last_24h[-5:]]
        diffs = [int(ts_values[i] - ts_values[i - 1]) for i in range(1, len(ts_values)) if ts_values[i] > ts_values[i - 1]]
        if diffs:
            avg_diff = int(sum(diffs) / len(diffs))
            recent_spacing = f"Ø letzte HTTP-Abstände: {avg_diff}s"
    quota_limit_24h = 50
    quota_used_24h = len(last_24h)
    quota_remaining_24h = max(0, quota_limit_24h - quota_used_24h)
    mqtt_rate_limited = store.next_retry_reason == "quota_exceeded" and store.next_retry_at > now
    if quota_used_24h > 0:
        api_quota_hint = (
            f"CarData API lokal: {quota_used_24h}/{quota_limit_24h} Requests im 24h-Fenster, ca. {quota_remaining_24h} übrig"
        )
    else:
        api_quota_hint = "CarData API lokal: noch keine belastbaren Add-on-Daten im 24h-Fenster"
    api_quota_note = (
        "Nur Add-on-lokale REST/API-Requests; frühere Stände und andere Clients sind darin nicht enthalten."
    )
    if mqtt_rate_limited:
        stream_status = "BMW-Streaming aktuell blockiert"
    elif store.next_retry_at > now:
        stream_status = "BMW-Streaming Reconnect wartet"
    else:
        stream_status = "BMW-Streaming aktuell nicht blockiert"
    stream_note = (
        "BMW dokumentiert 50 API-Requests pro Tag und empfiehlt für häufigeren Zugriff die CarData-Streaming-Lösung."
    )

    return {
        "request_count_24h": len(last_24h),
        "last_request_at": last_request_at,
        "last_request_summary": last_request_summary,
        "request_interval_hint": interval_hint,
        "recent_request_spacing": recent_spacing,
        "auth_poll_interval_s": auth_poll_interval_s,
        "quota_limit_24h": quota_limit_24h,
        "quota_used_24h": quota_used_24h,
        "quota_remaining_24h": quota_remaining_24h,
        "api_quota_hint": api_quota_hint,
        "api_quota_note": api_quota_note,
        "stream_status": stream_status,
        "stream_note": stream_note,
        "mqtt_rate_limited": mqtt_rate_limited,
    }


def should_auto_start() -> bool:
    return stream_mode() == "always_on" and within_active_window()


def push_sse(event: str, data: dict):
    msg = f"event: {event}\ndata: {json.dumps(data)}\n\n"
    for q in list(_sse_queues):
        try:
            q.put_nowait(msg)
        except queue.Full:
            _sse_queues.remove(q)


def on_bridge_status(status: str):
    push_sse("bridge_status", {"status": status})


def _clear_runtime_state():
    store.set(
        next_retry_at=0,
        next_retry_reason="",
        quota_error_count=0,
        last_quota_at=None,
        last_stream_connect_attempt_at=0,
        preferred_stream_transport="tcp",
        auth_poll_interval_s=0,
    )


def _stop_bridge():
    global bridge
    if bridge:
        bridge.stop()
        bridge = None


# ── Flask ─────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.urandom(32)


def B() -> str:
    return request.headers.get("X-Ingress-Path", INGRESS_PATH)


# ── HTML Template ─────────────────────────────────────────────────────────────
STYLE = """
<style>
:root{--bmw:#1c69d4;--dark:#0a1628;--ok:#16a34a;--err:#dc2626;--warn:#ca8a04;--bg:#eef2f8}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:var(--bg);color:#1a1a2e;min-height:100vh}
header{background:var(--dark);color:#fff;padding:.9rem 1.5rem;display:flex;align-items:center;gap:.9rem}
header h1{font-size:1.15rem;font-weight:700}
header small{opacity:.55;font-size:.75rem;display:block}
main{max-width:680px;margin:1.8rem auto;padding:0 1rem}
.card{background:#fff;border-radius:14px;box-shadow:0 2px 14px rgba(0,0,0,.07);padding:1.5rem;margin-bottom:1.4rem}
.card h2{font-size:.95rem;font-weight:700;color:var(--bmw);margin-bottom:1rem;display:flex;align-items:center;gap:.5rem}
label{display:block;font-size:.82rem;font-weight:600;margin:.8rem 0 .25rem;color:#374151}
input[type=text],input[type=password]{width:100%;border:1.5px solid #d1d5db;border-radius:8px;padding:.55rem .8rem;font-size:.92rem;transition:.15s}
input:focus{outline:none;border-color:var(--bmw);box-shadow:0 0 0 3px rgba(28,105,212,.15)}
.hint{font-size:.78rem;color:#6b7280;margin-top:.2rem}
.btn{display:inline-flex;align-items:center;gap:.4rem;padding:.55rem 1.3rem;border-radius:8px;border:none;cursor:pointer;font-weight:600;font-size:.88rem;transition:.18s;text-decoration:none}
.btn-primary{background:var(--bmw);color:#fff}.btn-primary:hover{background:#1558b8}
.btn-danger{background:var(--err);color:#fff}.btn-danger:hover{background:#b91c1c}
.btn-ghost{background:#f3f4f6;color:#374151}.btn-ghost:hover{background:#e5e7eb}
.btn-sm{padding:.3rem .8rem;font-size:.78rem}
.mt{margin-top:.8rem}
.step-list{list-style:none;padding:0}
.step-list li{display:flex;gap:.75rem;margin-bottom:.8rem;align-items:flex-start;font-size:.88rem}
.step-num{width:24px;height:24px;min-width:24px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:700;font-size:.75rem;color:#fff;margin-top:1px}
.sn-blue{background:var(--bmw)}.sn-ok{background:var(--ok)}.sn-grey{background:#9ca3af}
.code-block{background:#0f172a;color:#7dd3fc;font-family:monospace;font-size:1.5rem;letter-spacing:.25em;padding:.6rem 1.1rem;border-radius:8px;display:inline-block;margin:.5rem 0}
.url-chip{background:#f0f4ff;border:1px solid #bfdbfe;border-radius:6px;padding:.4rem .7rem;font-size:.82rem;font-family:monospace;word-break:break-all;display:block;margin:.4rem 0}
.badge{display:inline-block;padding:.2rem .65rem;border-radius:20px;font-size:.72rem;font-weight:700}
.b-ok{background:#dcfce7;color:#15803d}.b-err{background:#fee2e2;color:#b91c1c}
.b-warn{background:#fef9c3;color:#854d0e}.b-info{background:#dbeafe;color:#1d4ed8}
.alert{padding:.7rem 1rem;border-radius:8px;margin-bottom:1rem;font-size:.88rem}
.a-err{background:#fee2e2;color:#991b1b;border:1px solid #fca5a5}
.a-ok{background:#dcfce7;color:#14532d;border:1px solid #86efac}
.a-info{background:#dbeafe;color:#1e3a8a;border:1px solid #93c5fd}
.vehicle-grid{display:grid;gap:.75rem}
.vehicle-card{border:2px solid #e5e7eb;border-radius:10px;padding:.9rem 1.1rem;cursor:pointer;transition:.15s;display:flex;align-items:center;gap:.9rem}
.vehicle-card:hover{border-color:var(--bmw);background:#f0f4ff}
.vehicle-card.selected{border-color:var(--bmw);background:#eff6ff}
.vehicle-card input[type=checkbox]{width:18px;height:18px;accent-color:var(--bmw);cursor:pointer;flex-shrink:0}
.vc-info strong{font-size:.95rem}.vc-info small{font-size:.78rem;color:#6b7280}
.stat-row{display:grid;grid-template-columns:1fr 1fr 1fr;gap:.7rem;margin-bottom:.9rem}
.stat{background:#f8faff;border:1px solid #e0e7ff;border-radius:10px;padding:.75rem;text-align:center}
.stat-val{font-size:1.25rem;font-weight:800;color:var(--bmw)}.stat-lbl{font-size:.7rem;color:#6b7280}
.spinner{display:inline-block;width:16px;height:16px;border:2.5px solid #bfdbfe;border-top-color:var(--bmw);border-radius:50%;animation:spin .7s linear infinite;vertical-align:middle}
@keyframes spin{to{transform:rotate(360deg)}}
hr{border:none;border-top:1px solid #f1f1f1;margin:.9rem 0}
.mqtt-row{display:flex;justify-content:space-between;align-items:center;font-size:.83rem;padding:.3rem 0}
.mqtt-key{color:#6b7280}.mqtt-val{font-weight:600;font-family:monospace}
</style>
"""

BMW_LOGO = """
<svg width="30" height="30" viewBox="0 0 48 48">
  <circle cx="24" cy="24" r="22" fill="#1c69d4"/>
  <circle cx="24" cy="24" r="18" fill="none" stroke="white" stroke-width="1.5" opacity=".4"/>
  <path d="M24 6v18H6a18 18 0 0018-18z" fill="white" opacity=".85"/>
  <path d="M42 24H24V6a18 18 0 0118 18z" fill="#1c69d4"/>
  <path d="M24 42V24H42a18 18 0 01-18 18z" fill="white" opacity=".85"/>
  <path d="M6 24h18v18A18 18 0 016 24z" fill="#1c69d4"/>
</svg>"""

PAGE = STYLE + """
<header>
  <div style="display:flex;align-items:center;flex-shrink:0">{{ logo|safe }}</div>
  <div>
    <h1>BMW CarData Bridge</h1>
    <small>Home Assistant Addon v3</small>
    {% if phase == 'dashboard' and retry_hint %}
    <small id="hdr-retry" style="display:block;color:#fde68a;opacity:1;margin-top:.2rem">{{ retry_hint }}</small>
    {% endif %}
    {% if phase == 'dashboard' and block_hint %}
    <small id="hdr-block" style="display:block;color:#fca5a5;opacity:1;margin-top:.2rem">{{ block_hint }}</small>
    {% endif %}
  </div>
  {% if phase == 'dashboard' %}
  <span style="margin-left:auto" id="hdr-badge" class="badge {{ 'b-ok' if status=='connected' else 'b-warn' }}">{{ status }}</span>
  {% endif %}
</header>
<main>
  {% if error %}<div class="alert a-err">⚠ {{ error }}</div>{% endif %}
  {% if info %}<div class="alert a-info">ℹ {{ info }}</div>{% endif %}

  {# ── PHASE: mqtt_check ────────────────────────────────────────── #}
  {% if phase == 'mqtt_check' %}
  <div class="card">
    <h2>📡 MQTT-Broker Konfiguration</h2>
    <p style="font-size:.87rem;margin-bottom:1rem">
      Die Verbindungsdaten zum MQTT-Broker werden in den <strong>Addon-Optionen</strong> in Home Assistant konfiguriert
      (Seitenleiste → Add-ons → BMW CarData → Konfiguration).
    </p>
    <div class="mqtt-row"><span class="mqtt-key">Host</span><span class="mqtt-val">{{ opts.mqtt_host }}</span></div>
    <div class="mqtt-row"><span class="mqtt-key">Port</span><span class="mqtt-val">{{ opts.mqtt_port }}</span></div>
    <div class="mqtt-row"><span class="mqtt-key">Benutzer</span><span class="mqtt-val">{{ opts.mqtt_user or '(kein)' }}</span></div>
    <div class="mqtt-row"><span class="mqtt-key">Präfix</span><span class="mqtt-val">{{ opts.bmw_prefix }}</span></div>
    <hr>
    {% if mqtt_ok %}
    <div class="alert a-ok" style="margin-bottom:.8rem">✓ MQTT-Broker erreichbar</div>
    {% else %}
    <div class="alert a-err" style="margin-bottom:.8rem">✗ MQTT-Broker nicht erreichbar – bitte Addon-Optionen prüfen.</div>
    {% endif %}
    {% if mqtt_ok %}
    <a href="{{ B }}/setup" class="btn btn-primary">Weiter → BMW Login</a>
    {% else %}
    <a href="{{ B }}/" class="btn btn-ghost">↻ Erneut prüfen</a>
    {% endif %}
  </div>

  {# ── PHASE: setup ─────────────────────────────────────────────── #}
  {% elif phase == 'setup' %}
  <div class="card">
    <h2>🔑 BMW CarData Client-ID</h2>
    <ul class="step-list">
      <li><span class="step-num sn-blue">1</span><div>Öffne das BMW-Portal und melde dich an.</div></li>
      <li><span class="step-num sn-blue">2</span><div>Navigiere zu <strong>Persönliche Daten → Meine Fahrzeuge → CarData</strong></div></li>
      <li><span class="step-num sn-blue">3</span><div>Klicke <strong>„Client-ID erstellen"</strong> &nbsp;<em>(⚠ NICHT „Fahrzeug authentifizieren"!)</em></div></li>
      <li><span class="step-num sn-blue">4</span><div>Klicke <strong>„Datenauswahl ändern"</strong> und aktiviere alle gewünschten Datenpunkte.</div></li>
      <li><span class="step-num sn-blue">5</span><div>Öffne <strong>CarData Stream → Show Connection Details</strong> und kopiere dort zusätzlich den <strong>Username (GCID)</strong>.</div></li>
      <li><span class="step-num sn-blue">6</span><div>Trage Client-ID und GCID unten ein.</div></li>
    </ul>
    <form method="post" action="{{ B }}/setup">
      <label>Client-ID (GUID)</label>
      <input type="text" name="client_id" placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" required value="{{ client_id or '' }}">
      <div class="hint">Die 36-stellige ID aus dem BMW CarData Portal</div>
      <label>GCID / Username</label>
      <input type="text" name="gcid" placeholder="BMW CarData Stream Username" required value="{{ gcid or '' }}">
      <div class="hint">Der Username aus "Show Connection Details" im BMW CarData Bereich</div>
      <div class="mt"><button type="submit" class="btn btn-primary">→ BMW Login starten</button></div>
    </form>
  </div>

  {# ── PHASE: auth ──────────────────────────────────────────────── #}
  {% elif phase == 'auth' %}
  <div class="card">
    <h2>🔐 BMW Account Login</h2>
    <ul class="step-list">
      <li><span class="step-num sn-ok">✓</span><div>Client-ID gespeichert</div></li>
      <li><span class="step-num sn-blue">1</span>
        <div>
          Öffne jetzt den BMW-OneID-Login:<br>
          <a href="{{ login_url }}" target="_blank" rel="noopener" class="btn btn-primary btn-sm" style="margin:.45rem 0 .55rem 0">BMW Login öffnen</a><br>
          <span class="url-chip">{{ verification_host }}</span>
          <div class="hint">Falls sich kein Tab öffnet, rufe die Adresse manuell auf und gib anschließend den Code unten ein.</div>
        </div>
      </li>
      <li><span class="step-num sn-blue">2</span>
        <div>
          Gib diesen Code ein:<br>
          <span class="code-block">{{ user_code }}</span><br>
          <small style="color:#6b7280">Gültig für {{ expires_min }} Minuten</small>
        </div>
      </li>
      <li><span class="step-num sn-blue">3</span>
        <div>Melde dich mit deinem <strong>MyBMW-Account</strong> an und bestätige die Freigabe.<br>
          <em>Die Seite aktualisiert sich automatisch.</em>
        </div>
      </li>
    </ul>
    <div id="poll-status" class="mt" style="font-size:.87rem;color:#6b7280">
      <span class="spinner"></span> Warte auf BMW-Login …
    </div>
  </div>
  <script>
    const B = "{{ B }}";
    const es = new EventSource(B + "/poll");
    es.addEventListener("auth_ok", () => {
      document.getElementById("poll-status").innerHTML = '<span class="badge b-ok">✓ Login erfolgreich – Fahrzeuge werden geladen …</span>';
      setTimeout(() => location.href = B + "/pick", 1200);
    });
    es.addEventListener("auth_error", e => {
      const d = JSON.parse(e.data);
      document.getElementById("poll-status").innerHTML = '<span class="badge b-err">Fehler: ' + d.message + '</span>';
    });
  </script>

  {# ── PHASE: pick ──────────────────────────────────────────────── #}
  {% elif phase == 'pick' %}
  <div class="card">
    <h2>🚗 Fahrzeug auswählen</h2>
    <p style="font-size:.85rem;margin-bottom:1rem">
      {{ vehicles|length }} Fahrzeug{{ 'e' if vehicles|length != 1 else '' }} in deinem BMW-Account gefunden.
      Wähle alle aus, die in Home Assistant erscheinen sollen.
    </p>
    <form method="post" action="{{ B }}/pick">
      <div class="vehicle-grid">
        {% for v in vehicles %}
        <label class="vehicle-card" id="vc-{{ loop.index }}">
          <input type="checkbox" name="vins" value="{{ v.vin }}"
            {% if v.vin in preselected %}checked{% endif %}
            onchange="this.closest('.vehicle-card').classList.toggle('selected', this.checked)">
          <div class="vc-info">
            <strong>{{ v.model }}</strong><br>
            <small>VIN: {{ v.vin }}{% if v.year %} &nbsp;·&nbsp; {{ v.year }}{% endif %}{% if v.color %} &nbsp;·&nbsp; {{ v.color }}{% endif %}</small>
          </div>
        </label>
        {% endfor %}
      </div>
      <div class="mt">
        <button type="submit" class="btn btn-primary">→ Bridge starten</button>
      </div>
    </form>
  </div>
  <script>
    // Pre-mark selected
    document.querySelectorAll('.vehicle-card input:checked').forEach(cb =>
      cb.closest('.vehicle-card').classList.add('selected'));
  </script>

  {# ── PHASE: dashboard ─────────────────────────────────────────── #}
  {% elif phase == 'dashboard' %}
  <div class="card">
    <h2>📊 Verbindungsstatus</h2>
    {% if block_hint %}
    <div id="block-banner" class="alert a-err" style="margin-bottom:.8rem">⛔ {{ block_hint }}</div>
    {% endif %}
    {% if retry_hint %}
    <div id="retry-banner" class="alert a-info" style="margin-bottom:.8rem">⏳ {{ retry_hint }}</div>
    {% endif %}
    <div class="stat-row">
      <div class="stat"><div class="stat-val" id="msg-count">{{ msg_count }}</div><div class="stat-lbl">Nachrichten</div></div>
      <div class="stat"><div class="stat-val" style="font-size:.9rem" id="token-exp">{{ token_exp }}</div><div class="stat-lbl">Token gültig bis</div></div>
      <div class="stat"><div class="stat-val">{{ vehicles|length }}</div><div class="stat-lbl">Fahrzeug{{ 'e' if vehicles|length != 1 else '' }}</div></div>
    </div>
    <div id="window-state" style="font-size:.8rem;color:#6b7280;margin-top:.6rem">Zeitfenster: {{ active_window or 'immer aktiv' }}{% if not window_open %} · Nächstes Fenster: {{ next_window_start or '–' }}{% endif %}</div>
    <div id="block-state" style="font-size:.8rem;color:#6b7280;margin-top:.6rem">Startstatus: {{ block_reason or 'nicht blockiert' }}</div>
    <div id="rate-limit" style="font-size:.8rem;color:#6b7280;margin-top:.6rem">Nächster Retry: {{ next_retry_at or '–' }}{% if retry_countdown %} · {{ retry_countdown }}{% endif %}</div>
    <div id="last-connect" style="font-size:.8rem;color:#6b7280">Letzte BMW-Verbindung: {{ last_connected_at or '–' }}</div>
    <div id="last-quota" style="font-size:.8rem;color:#6b7280">Letztes Rate-Limit: {{ last_quota_at or '–' }}{% if quota_error_count %} ({{ quota_error_count }}x){% endif %}</div>
    <div id="last-msg" style="font-size:.78rem;color:#9ca3af">Letzte Nachricht: {{ last_msg or '–' }}</div>
  </div>

  <div class="card">
    <h2>🌐 BMW API & Stream</h2>
    <div id="api-quota" class="alert a-info" style="margin-bottom:.8rem">{{ api_quota_hint }}</div>
    <div id="stream-status-banner" class="alert {{ 'a-err' if mqtt_rate_limited else 'a-ok' }}" style="margin-bottom:.8rem">{{ stream_status }}</div>
    <div class="stat-row">
      <div class="stat"><div class="stat-val" id="req-count">{{ request_count_24h }}</div><div class="stat-lbl">API Requests / 24h</div></div>
      <div class="stat"><div class="stat-val" style="font-size:.9rem" id="req-interval">{{ auth_poll_interval_s ~ 's' if auth_poll_interval_s else '–' }}</div><div class="stat-lbl">Akt. Poll-Intervall</div></div>
      <div class="stat"><div class="stat-val" style="font-size:.9rem" id="req-last-at">{{ last_request_at or '–' }}</div><div class="stat-lbl">Letzter BMW Request</div></div>
    </div>
    <div id="req-hint" style="font-size:.8rem;color:#6b7280;margin-top:.6rem">API-Intervall: {{ request_interval_hint }}</div>
    <div id="req-spacing" style="font-size:.8rem;color:#6b7280">{{ recent_request_spacing or 'Ø letzte API-Abstände: –' }}</div>
    <div id="req-summary" style="font-size:.78rem;color:#9ca3af">Letzter Request: {{ last_request_summary or '–' }}</div>
    <div id="api-note" style="font-size:.78rem;color:#9ca3af">{{ api_quota_note }}</div>
    <div id="stream-note" style="font-size:.78rem;color:#9ca3af">{{ stream_note }}</div>
  </div>

  <div class="card">
    <h2>🚗 Fahrzeuge & Sensoren</h2>
    {% for v in vehicles %}
    <div style="display:flex;justify-content:space-between;align-items:center;padding:.4rem 0{% if not loop.last %};border-bottom:1px solid #f3f4f6{% endif %}">
      <div>
        <strong style="font-size:.9rem">{{ v.model }}</strong>
        <small style="display:block;color:#6b7280;font-size:.75rem">{{ v.vin }}</small>
      </div>
      <span class="badge b-info">{{ sensor_count }} Sensoren + GPS</span>
    </div>
    {% endfor %}
    <div class="mt" style="font-size:.8rem;color:#6b7280">
      → <a href="homeassistant://settings/integrations/integration/mqtt" style="color:var(--bmw)">Einstellungen → Geräte & Dienste → MQTT</a>
    </div>
  </div>

  <div class="card">
    <h2>⚙️ Verwaltung</h2>
    <div style="display:flex;gap:.6rem;flex-wrap:wrap">
      {% if stream_mode == 'conservative' %}
      <form method="post" action="{{ B }}/start_bridge" style="display:inline">
        <button type="submit" class="btn btn-primary btn-sm">▶ Bridge starten</button>
      </form>
      <form method="post" action="{{ B }}/stop_bridge" style="display:inline">
        <button type="submit" class="btn btn-ghost btn-sm">■ Bridge stoppen</button>
      </form>
      {% endif %}
      <a href="{{ B }}/pick_again" class="btn btn-ghost btn-sm">🚗 Fahrzeugauswahl ändern</a>
      <form method="post" action="{{ B }}/reload_vehicles" style="display:inline">
        <button type="submit" class="btn btn-ghost btn-sm">↻ Fahrzeuge neu laden</button>
      </form>
      <form method="post" action="{{ B }}/reset" style="display:inline" onsubmit="return confirm('Wirklich alle Tokens löschen und neu einrichten?')">
        <button type="submit" class="btn btn-danger btn-sm">🔄 Neu einrichten</button>
      </form>
    </div>
  </div>

  <script>
    const B = "{{ B }}";
    const es = new EventSource(B + "/events");
    es.addEventListener("bridge_status", e => {
      const d = JSON.parse(e.data);
      const badge = document.getElementById("hdr-badge");
      if(badge){ badge.textContent = d.status; badge.className = "badge " + (d.status==="connected"?"b-ok":"b-warn"); }
    });
    setInterval(() => {
      fetch(B + "/status_json").then(r=>r.json()).then(d=>{
        document.getElementById("msg-count").textContent = d.message_count;
        document.getElementById("last-msg").textContent  = "Letzte Nachricht: " + (d.last_message||"–");
        document.getElementById("token-exp").textContent = d.token_exp;
        document.getElementById("window-state").textContent = "Zeitfenster: " + (d.active_window || "immer aktiv") + (!d.window_open ? " · Nächstes Fenster: " + (d.next_window_start || "–") : "");
        document.getElementById("block-state").textContent = "Startstatus: " + (d.block_reason || "nicht blockiert");
        document.getElementById("rate-limit").textContent = "Nächster Retry: " + (d.next_retry_at||"–") + (d.retry_countdown ? " · " + d.retry_countdown : "");
        document.getElementById("last-connect").textContent = "Letzte BMW-Verbindung: " + (d.last_connected_at||"–");
        document.getElementById("last-quota").textContent = "Letztes Rate-Limit: " + (d.last_quota_at||"–") + (d.quota_error_count ? " (" + d.quota_error_count + "x)" : "");
        document.getElementById("req-count").textContent = d.request_count_24h;
        document.getElementById("req-interval").textContent = d.auth_poll_interval_s ? (d.auth_poll_interval_s + "s") : "–";
        document.getElementById("req-last-at").textContent = d.last_request_at || "–";
        document.getElementById("api-quota").textContent = d.api_quota_hint || "CarData API lokal: –";
        const streamBanner = document.getElementById("stream-status-banner");
        streamBanner.textContent = d.stream_status || "BMW-Streaming Status unbekannt";
        streamBanner.className = "alert " + (d.mqtt_rate_limited ? "a-err" : "a-ok");
        document.getElementById("req-hint").textContent = "API-Intervall: " + (d.request_interval_hint || "–");
        document.getElementById("req-spacing").textContent = d.recent_request_spacing || "Ø letzte API-Abstände: –";
        document.getElementById("req-summary").textContent = "Letzter Request: " + (d.last_request_summary || "–");
        document.getElementById("api-note").textContent = d.api_quota_note || "";
        document.getElementById("stream-note").textContent = d.stream_note || "";
        const hdrRetry = document.getElementById("hdr-retry");
        if(hdrRetry){ hdrRetry.textContent = d.retry_hint || ""; hdrRetry.style.display = d.retry_hint ? "block" : "none"; }
        const hdrBlock = document.getElementById("hdr-block");
        if(hdrBlock){ hdrBlock.textContent = d.block_hint || ""; hdrBlock.style.display = d.block_hint ? "block" : "none"; }
        const blockBanner = document.getElementById("block-banner");
        if(blockBanner){ blockBanner.textContent = d.block_hint ? "⛔ " + d.block_hint : ""; blockBanner.style.display = d.block_hint ? "block" : "none"; }
        const retryBanner = document.getElementById("retry-banner");
        if(retryBanner){ retryBanner.textContent = d.retry_hint ? "⏳ " + d.retry_hint : ""; retryBanner.style.display = d.retry_hint ? "block" : "none"; }
      });
    }, 10000);
  </script>
  {% endif %}
</main>
"""


def render(phase: str, **kw):
    opts = load_options()
    return render_template_string(
        PAGE,
        phase=phase, B=B(), logo=BMW_LOGO, opts=opts,
        error=kw.pop("error", None),
        info=kw.pop("info", None),
        **kw,
    )


# ── MQTT connectivity test ────────────────────────────────────────────────────

def test_mqtt(host: str, port: int, user: str, pw: str) -> bool:
    import paho.mqtt.client as mqtt
    result = {"ok": False}
    ev = threading.Event()
    def on_connect(c, ud, f, rc, p=None):
        result["ok"] = (rc == 0)
        ev.set()
        c.disconnect()
    c = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if user:
        c.username_pw_set(user, pw)
    try:
        c.on_connect = on_connect
        c.connect_async(host, port, keepalive=5)
        c.loop_start()
        ev.wait(timeout=6)
        c.loop_stop()
    except Exception:
        pass
    return result["ok"]


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    # If bridge is running → dashboard
    if bridge and getattr(bridge, "is_running", False):
        return _dashboard()
    # If tokens exist but no bridge → restart bridge
    if store.has_tokens:
        ov = load_override()
        if ov.get("selected_vins"):
            if should_auto_start():
                _maybe_start_bridge()
            return _dashboard()
        return redirect(B() + "/pick")
    # First time: MQTT check
    opts = load_options()
    ok = test_mqtt(opts.get("mqtt_host","core-mosquitto"),
                   opts.get("mqtt_port", 1883),
                   opts.get("mqtt_user",""),
                   opts.get("mqtt_password",""))
    return render("mqtt_check", mqtt_ok=ok)


def _dashboard():
    return render("dashboard", **_dashboard_context())


@app.route("/setup")
def setup_get():
    ov = load_override()
    return render("setup", client_id=ov.get("client_id",""), gcid=ov.get("gcid", ""))


@app.route("/setup", methods=["POST"])
def setup_post():
    client_id = request.form.get("client_id","").strip()
    gcid = request.form.get("gcid", "").strip()
    if len(client_id) < 10:
        return render("setup", error="Bitte eine gültige Client-ID eingeben.", client_id=client_id, gcid=gcid)
    if len(gcid) < 3:
        return render("setup", error="Bitte eine gültige GCID eingeben.", client_id=client_id, gcid=gcid)
    save_override({"client_id": client_id, "gcid": gcid})
    # Start device flow
    try:
        flow = BMWDeviceFlow(client_id, store, gcid=gcid)
        info = flow.start()
        session["flow_active"] = True
        threading.Thread(target=_poll_loop, args=(flow,), daemon=True).start()
        return render("auth",
                      user_code=info["user_code"],
                      login_url=info["verification_uri"],
                      verification_host=info["verification_host"],
                      verification_uri=info["verification_uri"],
                      expires_min=info["expires_in"]//60)
    except Exception as exc:
        return render("setup", error=f"BMW-Verbindung fehlgeschlagen: {exc}", client_id=client_id, gcid=gcid)


def _poll_loop(flow: BMWDeviceFlow):
    deadline = time.time() + 300
    while time.time() < deadline:
        time.sleep(flow.interval_seconds)
        try:
            tokens = flow.poll()
            if tokens:
                store.clear_auth_poll_interval()
                push_sse("auth_ok", {})
                return
        except Exception as exc:
            store.clear_auth_poll_interval()
            push_sse("auth_error", {"message": str(exc)})
            return
    store.clear_auth_poll_interval()
    push_sse("auth_error", {"message": "Zeitüberschreitung – bitte erneut versuchen."})


@app.route("/pick")
def pick_get():
    if not store.has_tokens:
        return redirect(B() + "/setup")
    ov = load_override()
    force_refresh = request.args.get("refresh") == "1"
    if ov.get("vehicles") and not force_refresh:
        preselected = ov.get("selected_vins", [v["vin"] for v in ov.get("vehicles", [])])
        return render("pick", vehicles=ov.get("vehicles", []), preselected=preselected, info="Fahrzeugliste aus lokalem Cache geladen.")
    try:
        vehicles = fetch_vehicles(store, force_refresh=force_refresh)
    except BMWAuthError as exc:
        return render("setup", error=f"Fahrzeugliste fehlgeschlagen: {exc}")
    save_override({"vehicles": vehicles})
    preselected = ov.get("selected_vins", [v["vin"] for v in vehicles])
    return render("pick", vehicles=vehicles, preselected=preselected)


@app.route("/pick", methods=["POST"])
def pick_post():
    vins = request.form.getlist("vins")
    if not vins:
        return render("pick", vehicles=load_override().get("vehicles",[]),
                      preselected=[], error="Bitte mindestens ein Fahrzeug auswählen.")
    save_override({"selected_vins": vins})
    if should_auto_start():
        _maybe_start_bridge()
    return redirect(B() + "/")


@app.route("/pick_again")
def pick_again():
    return redirect(B() + "/pick")


@app.route("/reload_vehicles", methods=["POST"])
def reload_vehicles():
    if not store.has_tokens:
        return redirect(B() + "/setup")
    try:
        vehicles = fetch_vehicles(store, force_refresh=True)
    except BMWAuthError as exc:
        return render("setup", error=f"Fahrzeugliste fehlgeschlagen: {exc}")
    save_override({"vehicles": vehicles})
    return redirect(B() + "/pick")


@app.route("/start_bridge", methods=["POST"])
def start_bridge():
    _maybe_start_bridge(force=True)
    return redirect(B() + "/")


@app.route("/stop_bridge", methods=["POST"])
def stop_bridge():
    _stop_bridge()
    return redirect(B() + "/")


# ── SSE endpoints ─────────────────────────────────────────────────────────────

def _sse_stream():
    q: queue.Queue = queue.Queue(maxsize=20)
    _sse_queues.append(q)
    def generate():
        while True:
            try:
                yield q.get(timeout=30)
            except queue.Empty:
                yield "event: heartbeat\ndata: {}\n\n"
    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})


@app.route("/poll")
def poll_sse():
    return _sse_stream()


@app.route("/events")
def events_sse():
    return _sse_stream()


@app.route("/status_json")
def status_json():
    b      = bridge
    exp_ts = store.expires_at
    exp_str = _format_local_timestamp(exp_ts) if exp_ts else "?"
    retry_state = _retry_ui_state()
    block_state = _block_reason_state()
    request_state = _request_telemetry()
    return jsonify({
        "status":        b.status if b else "stopped",
        "message_count": b.message_count if b else 0,
        "last_message":  b.last_message if b else None,
        "token_exp":     exp_str,
        "active_window": active_window(),
        "window_open": within_active_window(),
        "next_window_start": next_window_start(),
        "next_retry_at": retry_state["next_retry_at"],
        "retry_countdown": retry_state["retry_countdown"],
        "retry_hint": retry_state["retry_hint"],
        "block_reason": block_state["block_reason"],
        "block_hint": block_state["block_hint"],
        "last_connected_at": _format_local_iso(store.last_connected_at),
        "last_quota_at": _format_local_iso(store.last_quota_at),
        "quota_error_count": store.quota_error_count,
        "request_count_24h": request_state["request_count_24h"],
        "last_request_at": request_state["last_request_at"],
        "last_request_summary": request_state["last_request_summary"],
        "request_interval_hint": request_state["request_interval_hint"],
        "recent_request_spacing": request_state["recent_request_spacing"],
        "auth_poll_interval_s": request_state["auth_poll_interval_s"],
        "quota_limit_24h": request_state["quota_limit_24h"],
        "quota_used_24h": request_state["quota_used_24h"],
        "quota_remaining_24h": request_state["quota_remaining_24h"],
        "api_quota_hint": request_state["api_quota_hint"],
        "api_quota_note": request_state["api_quota_note"],
        "stream_status": request_state["stream_status"],
        "stream_note": request_state["stream_note"],
        "mqtt_rate_limited": request_state["mqtt_rate_limited"],
    })


@app.route("/reset", methods=["POST"])
def reset():
    _stop_bridge()
    _clear_runtime_state()
    store.clear()
    if os.path.exists(OVERRIDE_FILE):
        os.remove(OVERRIDE_FILE)
    session.clear()
    return redirect(B() + "/")


# ── Bridge control ────────────────────────────────────────────────────────────

def _dashboard_context() -> dict:
    ov = load_override()
    exp_ts = store.expires_at
    exp_str = _format_local_timestamp(exp_ts) if exp_ts else "?"
    retry_state = _retry_ui_state()
    block_state = _block_reason_state()
    request_state = _request_telemetry()
    sel_vins = ov.get("selected_vins", [])
    vehicles = [v for v in (ov.get("vehicles") or []) if v["vin"] in sel_vins]
    return {
        "status": bridge.status if bridge else "stopped",
        "msg_count": bridge.message_count if bridge else 0,
        "last_msg": bridge.last_message if bridge else None,
        "token_exp": exp_str,
        "active_window": active_window(),
        "window_open": within_active_window(),
        "next_window_start": next_window_start(),
        "next_retry_at": retry_state["next_retry_at"],
        "retry_countdown": retry_state["retry_countdown"],
        "retry_hint": retry_state["retry_hint"],
        "block_reason": block_state["block_reason"],
        "block_hint": block_state["block_hint"],
        "last_connected_at": _format_local_iso(store.last_connected_at),
        "last_quota_at": _format_local_iso(store.last_quota_at),
        "quota_error_count": store.quota_error_count,
        "request_count_24h": request_state["request_count_24h"],
        "last_request_at": request_state["last_request_at"],
        "last_request_summary": request_state["last_request_summary"],
        "request_interval_hint": request_state["request_interval_hint"],
        "recent_request_spacing": request_state["recent_request_spacing"],
        "auth_poll_interval_s": request_state["auth_poll_interval_s"],
        "quota_limit_24h": request_state["quota_limit_24h"],
        "quota_used_24h": request_state["quota_used_24h"],
        "quota_remaining_24h": request_state["quota_remaining_24h"],
        "api_quota_hint": request_state["api_quota_hint"],
        "api_quota_note": request_state["api_quota_note"],
        "stream_status": request_state["stream_status"],
        "stream_note": request_state["stream_note"],
        "mqtt_rate_limited": request_state["mqtt_rate_limited"],
        "stream_mode": stream_mode(),
        "vehicles": vehicles,
        "sensor_count": len(BMWMQTTBridge.SENSORS),
    }


def _maybe_start_bridge(force: bool = False):
    global bridge
    opts  = load_options()
    ov    = load_override()
    vins  = ov.get("selected_vins", [])
    all_v = ov.get("vehicles", [])
    sel_vehicles = [v for v in all_v if v["vin"] in vins]
    if ov.get("gcid") and not store.gcid:
        store.save({"gcid": ov["gcid"]})

    if not sel_vehicles or not store.has_tokens:
        return

    if not force and not within_active_window():
        log.info("BMW bridge start skipped: outside configured active window")
        return

    if not force and not should_auto_start():
        return

    if bridge and getattr(bridge, "is_running", False):
        return

    if bridge:
        bridge.stop()

    bridge = BMWMQTTBridge(
        store=store,
        vehicles=sel_vehicles,
        local_host=opts.get("mqtt_host","core-mosquitto"),
        local_port=opts.get("mqtt_port", 1883),
        local_user=opts.get("mqtt_user",""),
        local_password=opts.get("mqtt_password",""),
        local_prefix=opts.get("bmw_prefix","bmw/"),
        on_status_change=on_bridge_status,
    )
    bridge.start()
    log.info("Bridge started for %d vehicle(s): %s", len(sel_vehicles), vins)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s – %(message)s")
    # Auto-start bridge if fully configured
    if should_auto_start() and store.has_tokens and load_override().get("selected_vins"):
        log.info("Auto-starting bridge from saved config …")
        _maybe_start_bridge()
    app.run(host="0.0.0.0", port=8099, threaded=True)
