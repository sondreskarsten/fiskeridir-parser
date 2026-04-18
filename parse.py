"""Parser: raw vessels.jsonl.gz -> admitted snapshot + unified 12-col changelog.

Env:
    GCS_BUCKET           default sondre_brreg_data
    GCS_PREFIX           default fiskeridir
    GCP_PROJECT          default sondreskarsten-d7d14
    TARGET_DATE          yyyy-mm-dd, default today
    RUN_MODE             bootstrap / daily (default daily)
    STATE_DIR            local working dir (default /data)
    SECRET_CLIENT_ID     Secret Manager name for BW client id   (default barentswatch-client-id)
    SECRET_CLIENT_SECRET Secret Manager name for BW client secret (default barentswatch-client-secret)

GCS output:
    gs://{bucket}/{prefix}/parsed/v1/state/{date}.parquet        latest snapshot (admitted rows)
    gs://{bucket}/{prefix}/cdc/changelog/{date}.parquet          unified-12col events
    gs://{bucket}/{prefix}/parsed/v1/drops/{date}.jsonl          admission drop log
    gs://{bucket}/{prefix}/parsed/v1/meta/{date}.json            run metadata
"""

import gzip
import io
import json
import os
import sys
import uuid
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from admission import admit
from flatten import flatten
from enrich import build_callsign_index, enrich
from cdc import diff
from schema import (
    SNAPSHOT_SCHEMA, CHANGELOG_SCHEMA, ALL_SNAPSHOT_COLS, VALUE_COLS, DATA_SOURCE,
)
from barentswatch import BarentsWatchClient


GCS_BUCKET = os.environ.get("GCS_BUCKET", "sondre_brreg_data")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "fiskeridir")
GCP_PROJECT = os.environ.get("GCP_PROJECT", "sondreskarsten-d7d14")
TARGET_DATE = os.environ.get("TARGET_DATE", date.today().isoformat())
RUN_MODE = os.environ.get("RUN_MODE", "daily")
STATE_DIR = os.environ.get("STATE_DIR", "/data")
SECRET_CLIENT_ID = os.environ.get("SECRET_CLIENT_ID", "barentswatch-client-id")
SECRET_CLIENT_SECRET = os.environ.get("SECRET_CLIENT_SECRET", "barentswatch-client-secret")


def _load_raw(bucket, target_date):
    blob = bucket.blob(f"{GCS_PREFIX}/raw/{target_date}/vessels.jsonl.gz")
    if not blob.exists():
        print(f"  ERROR: raw missing at gs://{GCS_BUCKET}/{blob.name}", flush=True)
        sys.exit(1)
    data = blob.download_as_bytes()
    vessels = []
    with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as gz:
        for line in gz:
            vessels.append(json.loads(line))
    return vessels


def _find_latest_prior_snapshot_blob(bucket, target_date):
    blobs = list(bucket.list_blobs(prefix=f"{GCS_PREFIX}/parsed/v1/state/"))
    dated = []
    for b in blobs:
        fn = os.path.basename(b.name)
        if fn.endswith(".parquet"):
            datepart = fn[:-len(".parquet")]
            if datepart < target_date:
                dated.append((datepart, b))
    if not dated:
        return None
    dated.sort(key=lambda x: x[0])
    return dated[-1][1]


def _load_prior_snapshot(bucket, target_date):
    blob = _find_latest_prior_snapshot_blob(bucket, target_date)
    if blob is None:
        return []
    local = os.path.join(STATE_DIR, "prev_state.parquet")
    blob.download_to_filename(local)
    tbl = pq.read_table(local)
    return tbl.to_pylist()


def _write_snapshot(rows, local_path):
    if rows:
        tbl = pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA)
    else:
        tbl = pa.Table.from_pylist([], schema=SNAPSHOT_SCHEMA)
    pq.write_table(tbl, local_path, compression="zstd")


def _write_changelog(events, local_path):
    if events:
        tbl = pa.Table.from_pylist(events, schema=CHANGELOG_SCHEMA)
    else:
        tbl = pa.Table.from_pylist([], schema=CHANGELOG_SCHEMA)
    pq.write_table(tbl, local_path, compression="zstd")


def main():
    started_at = datetime.now(timezone.utc)
    run_id = str(uuid.uuid4())
    os.makedirs(STATE_DIR, exist_ok=True)

    print("=" * 60, flush=True)
    print(f"  fiskeridir-parser v2", flush=True)
    print(f"  run_id: {run_id}", flush=True)
    print(f"  date:   {TARGET_DATE}", flush=True)
    print(f"  mode:   {RUN_MODE}", flush=True)
    print(f"  GCS:    gs://{GCS_BUCKET}/{GCS_PREFIX}/", flush=True)
    print("=" * 60, flush=True)

    gcs = storage.Client()
    bucket = gcs.bucket(GCS_BUCKET)

    print("\n  Loading raw vessels...", flush=True)
    raw = _load_raw(bucket, TARGET_DATE)
    print(f"    raw count: {len(raw):,}", flush=True)

    print("\n  Applying universe admission...", flush=True)
    admitted, drops = admit(raw)
    drop_counts = {}
    for d in drops:
        drop_counts[d["reason"]] = drop_counts.get(d["reason"], 0) + 1
    print(f"    admitted: {len(admitted):,}", flush=True)
    print(f"    dropped:  {len(drops):,}", flush=True)
    for reason, n in sorted(drop_counts.items()):
        print(f"      {reason}: {n:,}", flush=True)

    print("\n  Flattening to snapshot rows...", flush=True)
    rows = flatten(admitted)
    print(f"    rows: {len(rows):,}", flush=True)

    print("\n  Loading prior snapshot...", flush=True)
    prev_rows = _load_prior_snapshot(bucket, TARGET_DATE)
    prev_by_key = {(r["orgnr"], r["vessel_id"]): r for r in prev_rows}
    print(f"    prior rows: {len(prev_rows):,}", flush=True)

    print("\n  Pulling BarentsWatch latest AIS...", flush=True)
    bw = BarentsWatchClient(GCP_PROJECT, SECRET_CLIENT_ID, SECRET_CLIENT_SECRET)
    try:
        latest = bw.latest_ais()
        print(f"    latest_ais records: {len(latest):,}", flush=True)
        cs_index = build_callsign_index(latest)
        print(f"    distinct callsigns: {len(cs_index):,}", flush=True)
    except Exception as e:
        print(f"    WARN: BarentsWatch fetch failed ({e}); proceeding without fresh positions", flush=True)
        cs_index = {}

    print("\n  Enriching snapshot with AIS positions...", flush=True)
    enrich_stats = enrich(rows, cs_index, prev_by_key)
    print(f"    fresh:           {enrich_stats['fresh']:,}", flush=True)
    print(f"    carried forward: {enrich_stats['carried_forward']:,}", flush=True)
    print(f"    no position:     {enrich_stats['no_position']:,}", flush=True)

    valid_time = datetime.fromisoformat(TARGET_DATE).replace(tzinfo=timezone.utc)
    detected_time = started_at

    print("\n  Computing CDC diff...", flush=True)
    bootstrap = (RUN_MODE == "bootstrap") or (len(prev_rows) == 0)
    events = diff(
        prev_rows, rows,
        target_date=datetime.fromisoformat(TARGET_DATE).date(),
        valid_time=valid_time,
        detected_time=detected_time,
        run_mode=RUN_MODE,
        run_id=run_id,
        bootstrap=bootstrap,
    )
    tally = {}
    for e in events:
        k = (e["event_type"], e["event_subtype"])
        tally[k] = tally.get(k, 0) + 1
    print(f"    events: {len(events):,}", flush=True)
    for (et, st), n in sorted(tally.items()):
        print(f"      {et:<14} {st:<14} {n:,}", flush=True)

    print("\n  Writing outputs...", flush=True)
    snap_local = os.path.join(STATE_DIR, "snapshot.parquet")
    cl_local = os.path.join(STATE_DIR, "changelog.parquet")
    _write_snapshot(rows, snap_local)
    _write_changelog(events, cl_local)

    snap_blob = bucket.blob(f"{GCS_PREFIX}/parsed/v1/state/{TARGET_DATE}.parquet")
    snap_blob.upload_from_filename(snap_local)
    snap_blob.reload()
    print(f"    snapshot: {snap_blob.size:,} bytes ({len(rows):,} rows)", flush=True)

    cl_blob = bucket.blob(f"{GCS_PREFIX}/cdc/changelog/{TARGET_DATE}.parquet")
    cl_blob.upload_from_filename(cl_local)
    cl_blob.reload()
    print(f"    changelog: {cl_blob.size:,} bytes ({len(events):,} rows)", flush=True)

    drops_local = os.path.join(STATE_DIR, "drops.jsonl")
    with open(drops_local, "w", encoding="utf-8") as f:
        for d in drops:
            f.write(json.dumps(d, ensure_ascii=False, default=str) + "\n")
    bucket.blob(f"{GCS_PREFIX}/parsed/v1/drops/{TARGET_DATE}.jsonl").upload_from_filename(drops_local)
    print(f"    drops log: {len(drops):,} entries", flush=True)

    finished_at = datetime.now(timezone.utc)
    meta = {
        "run_id": run_id,
        "run_mode": RUN_MODE,
        "target_date": TARGET_DATE,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "raw_count": len(raw),
        "admitted_count": len(rows),
        "dropped_count": len(drops),
        "drop_breakdown": drop_counts,
        "prior_snapshot_rows": len(prev_rows),
        "events_emitted": len(events),
        "event_tally": {f"{et}|{st}": n for (et, st), n in tally.items()},
        "ais_enrichment": enrich_stats,
        "bootstrap": bootstrap,
    }
    bucket.blob(f"{GCS_PREFIX}/parsed/v1/meta/{TARGET_DATE}.json").upload_from_string(
        json.dumps(meta, indent=2, default=str), content_type="application/json"
    )

    print(f"\n  Runtime: {(finished_at - started_at).total_seconds():.1f}s", flush=True)


if __name__ == "__main__":
    main()
