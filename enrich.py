"""Populate AIS position attributes on admitted snapshot rows.

Joins on radio_call_sign. If today's BarentsWatch /v1/latest/ais contains a
matching record, the snapshot row gets fresh (lat, lon, ts). If not, prior
values are carried forward from the previous snapshot — the platform's
"if same as yesterday, no movement" semantics.

No positions are persisted outside the snapshot.
"""

from datetime import datetime, timezone


def _parse_ts(s):
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except ValueError:
        return None


def build_callsign_index(latest_ais_records):
    """Return {callSign_upper: (lat, lon, ts_utc)} from BarentsWatch latest snapshot.

    Collisions: if one callSign appears more than once, keep the most recent.
    """
    idx = {}
    for r in latest_ais_records:
        cs = r.get("callSign")
        if not cs:
            continue
        lat = r.get("latitude")
        lon = r.get("longitude")
        if lat is None or lon is None:
            continue
        ts = _parse_ts(r.get("msgtime"))
        if ts is None:
            continue
        cs_up = cs.strip().upper()
        existing = idx.get(cs_up)
        if existing is None or ts > existing[2]:
            idx[cs_up] = (float(lat), float(lon), ts)
    return idx


def enrich(rows, callsign_index, prev_snapshot_by_key):
    """Mutate rows in place to fill in position attributes.

    Priority per row:
      1. callSign present in today's AIS snapshot -> fresh values
      2. Prior snapshot has prior values for same (orgnr, vessel_id) -> carry forward
      3. Leave as null
    """
    n_fresh = 0
    n_carry = 0
    n_null = 0
    for r in rows:
        cs = r.get("radio_call_sign")
        hit = callsign_index.get(cs.upper()) if cs else None
        if hit:
            r["last_lat"], r["last_lon"], r["last_position_ts"] = hit
            n_fresh += 1
            continue
        key = (r["orgnr"], r["vessel_id"])
        prev = prev_snapshot_by_key.get(key)
        if prev and prev.get("last_position_ts") is not None:
            r["last_lat"] = prev.get("last_lat")
            r["last_lon"] = prev.get("last_lon")
            r["last_position_ts"] = prev.get("last_position_ts")
            n_carry += 1
            continue
        n_null += 1
    return {"fresh": n_fresh, "carried_forward": n_carry, "no_position": n_null}
