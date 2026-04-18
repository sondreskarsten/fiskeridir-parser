"""Snapshot and changelog schema definitions for the fiskeridir vessel LUAS.

LUAS key: (orgnr, vessel_id)
Attributes: Merkeregisteret facts + last observed AIS position.
"""

import pyarrow as pa


KEY_COLS = ["orgnr", "vessel_id"]

# Merkeregisteret attributes carried in the snapshot
REGISTRY_VALUE_COLS = [
    "name",
    "length",
    "build_year",
    "rebuild_year",
    "engine_power_kw",
    "engine_build_year",
    "radio_call_sign",
    "municipality_code",
    "tonnage_gt",
    "tonnage_type",
]

# AIS-derived position attributes carried in the snapshot
POSITION_VALUE_COLS = [
    "last_lat",
    "last_lon",
    "last_position_ts",
]

VALUE_COLS = REGISTRY_VALUE_COLS + POSITION_VALUE_COLS

ALL_SNAPSHOT_COLS = KEY_COLS + VALUE_COLS

SNAPSHOT_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("vessel_id", pa.string()),
    ("name", pa.string()),
    ("length", pa.float64()),
    ("build_year", pa.int64()),
    ("rebuild_year", pa.int64()),
    ("engine_power_kw", pa.int64()),
    ("engine_build_year", pa.int64()),
    ("radio_call_sign", pa.string()),
    ("municipality_code", pa.string()),
    ("tonnage_gt", pa.int64()),
    ("tonnage_type", pa.string()),
    ("last_lat", pa.float64()),
    ("last_lon", pa.float64()),
    ("last_position_ts", pa.timestamp("us", tz="UTC")),
])

CHANGELOG_SCHEMA = pa.schema([
    ("orgnr", pa.string()),
    ("document_id", pa.string()),
    ("data_source", pa.string()),
    ("event_type", pa.string()),
    ("event_subtype", pa.string()),
    ("summary", pa.string()),
    ("changed_fields", pa.string()),
    ("valid_time", pa.timestamp("us", tz="UTC")),
    ("detected_time", pa.timestamp("us", tz="UTC")),
    ("details_json", pa.large_string()),
    ("source_run_mode", pa.string()),
    ("run_id", pa.string()),
])

DATA_SOURCE = "fiskeridir_vessel"
