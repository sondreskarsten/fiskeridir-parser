"""Flatten admitted raw vessels into snapshot rows (registry attributes only).

Consumes output of admission.admit(). Produces dicts ready for snapshot schema.
AIS position attributes (last_lat, last_lon, last_position_ts) are populated
downstream by the enrichment step.
"""


def _i(v):
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _f(v):
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _s(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def flatten(admitted_vessels):
    rows = []
    for v in admitted_vessels:
        rows.append({
            "orgnr": v["_orgnr"],
            "vessel_id": _s(v.get("id")),
            "name": _s(v.get("name")),
            "length": _f(v.get("length")),
            "build_year": _i(v.get("buildYear")),
            "rebuild_year": _i(v.get("rebuildYear")),
            "engine_power_kw": _i(v.get("enginePower")),
            "engine_build_year": _i(v.get("engineBuildYear")),
            "radio_call_sign": _s(v.get("radioCallSign")),
            "municipality_code": _s(v.get("municipalityCode")),
            "tonnage_gt": _i(v.get("tonnage")),
            "tonnage_type": _s(v.get("tonnageType")),
            "last_lat": None,
            "last_lon": None,
            "last_position_ts": None,
        })
    return rows
