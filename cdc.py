"""CDC diff for the vessel LUAS.

Compares prior snapshot (prev) to current (curr) by key (orgnr, vessel_id).
Emits unified-12col events:

  - new          : key present in curr, absent in prev
  - disappeared  : key present in prev, absent in curr
  - reappeared   : key present in curr+prev, and prev.n_gaps indicates gap
                   (we don't maintain gaps table here; treat as new if prev
                   was marked disappeared in pool; simpler: just 'new' if
                   not continuously present — handled via pool logic)
  - modified     : key present in both, values differ
                   event_subtype=vessel_admin if ONLY admin-flagged fields
                   changed; else vessel.

Admin-flagged fields:
  - municipality_code on kommunereform dates
  - tonnage_type when flipped without tonnage_gt changing

For simplicity in this iteration: tonnage_type flip-only -> vessel_admin.
municipality_code mutations: treat as vessel_admin only if target_date in
KOMMUNEREFORM_DATES set.
"""

import hashlib
import json
from datetime import date

from schema import (
    KEY_COLS, VALUE_COLS, REGISTRY_VALUE_COLS, POSITION_VALUE_COLS,
    DATA_SOURCE,
)


KOMMUNEREFORM_DATES = {
    date(2018, 1, 1),
    date(2020, 1, 1),
    date(2024, 1, 1),
}


def _row_hash(row):
    payload = json.dumps({c: row.get(c) for c in VALUE_COLS}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _key(row):
    return (row["orgnr"], row["vessel_id"])


def index_by_key(rows):
    out = {}
    for r in rows:
        out[_key(r)] = r
    return out


def _changed_fields(prev, curr):
    return [c for c in VALUE_COLS if (prev.get(c) if prev else None) != curr.get(c)]


def _classify_modified(changed, target_date):
    """Return event_subtype: 'vessel_admin' if ALL changed fields are admin artifacts,
    else 'vessel'.
    """
    if not changed:
        return "vessel"
    admin_fields = set()
    if "municipality_code" in changed and target_date in KOMMUNEREFORM_DATES:
        admin_fields.add("municipality_code")
    if "tonnage_type" in changed and "tonnage_gt" not in changed:
        admin_fields.add("tonnage_type")
    if admin_fields == set(changed):
        return "vessel_admin"
    return "vessel"


def _is_registry_only_change(changed):
    return any(c in REGISTRY_VALUE_COLS for c in changed)


def _make_event(row, event_type, event_subtype, changed, valid_time, detected_time, run_mode, run_id):
    orgnr, vessel_id = _key(row)
    document_id = f"{orgnr}|{vessel_id}"
    if event_type == "new":
        summary = f"new vessel {vessel_id} owned by {orgnr}: {row.get('name') or '(unnamed)'}"
    elif event_type == "disappeared":
        summary = f"vessel {vessel_id} no longer in registry for {orgnr}"
    else:
        summary = f"vessel {vessel_id} modified for {orgnr}: {','.join(changed)}"
    return {
        "orgnr": orgnr,
        "document_id": document_id,
        "data_source": DATA_SOURCE,
        "event_type": event_type,
        "event_subtype": event_subtype,
        "summary": summary,
        "changed_fields": json.dumps(changed),
        "valid_time": valid_time,
        "detected_time": detected_time,
        "details_json": json.dumps({c: row.get(c) for c in VALUE_COLS}, ensure_ascii=False, default=str),
        "source_run_mode": run_mode,
        "run_id": run_id,
    }


def diff(prev_rows, curr_rows, target_date, valid_time, detected_time, run_mode, run_id,
         bootstrap=False):
    prev_idx = index_by_key(prev_rows)
    curr_idx = index_by_key(curr_rows)

    events = []

    if bootstrap:
        for k, r in curr_idx.items():
            events.append(_make_event(r, "new", "vessel", [],
                                      valid_time, detected_time, run_mode, run_id))
        return events

    for k, r in curr_idx.items():
        if k not in prev_idx:
            events.append(_make_event(r, "new", "vessel", [],
                                      valid_time, detected_time, run_mode, run_id))
            continue
        prev = prev_idx[k]
        if _row_hash(prev) != _row_hash(r):
            changed = _changed_fields(prev, r)
            if not _is_registry_only_change(changed) and all(c in POSITION_VALUE_COLS for c in changed):
                continue
            registry_changed = [c for c in changed if c in REGISTRY_VALUE_COLS]
            if not registry_changed:
                continue
            subtype = _classify_modified(registry_changed, target_date)
            events.append(_make_event(r, "modified", subtype, registry_changed,
                                      valid_time, detected_time, run_mode, run_id))

    for k, r in prev_idx.items():
        if k not in curr_idx:
            events.append(_make_event(r, "disappeared", "vessel", [],
                                      valid_time, detected_time, run_mode, run_id))

    return events
