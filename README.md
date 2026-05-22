# fiskeridir-parser

Pattern A bulk-diff CDC for the Norwegian fishing vessel registry. Reads daily raw dumps from fiskeridir-collector, flattens, admits to universe, diffs against prior snapshot, emits a 12-column unified changelog.

## LUAS

**(orgnr, vessel_id)**. One row in the snapshot = one vessel owned by one company. The same physical hull (vessel_id) appears under different orgnrs over time as ownership transfers happen.

## Snapshot schema

| Column | Type | Nullable | Notes |
|---|---|---|---|
| `orgnr` | string | no | 9-digit company ID (key) |
| `vessel_id` | string | no | Fiskeridir hull ID (key) |
| `name` | string | no | Vessel name |
| `length` | float64 | yes | Greatest length in meters |
| `build_year` | int32 | yes | |
| `rebuild_year` | int32 | yes | NULL if never rebuilt |
| `engine_power_kw` | int32 | yes | |
| `engine_build_year` | int32 | yes | |
| `radio_call_sign` | string | yes | NULL for ~10% of small boats |
| `municipality_code` | string | yes | 4-digit kommune code |
| `tonnage_gt` | float64 | yes | Gross tonnage (normalized) |
| `tonnage_type` | string | yes | "1969" or "other" |
| `last_lat` | float64 | yes | From BarentsWatch enrichment (currently NULL) |
| `last_lon` | float64 | yes | |
| `last_position_ts` | timestamp | yes | |

**Cardinality**: ~4,100 rows/day (after admission drops ~500 person-only + ~50 multi-owner + ~15 duplicate hulls).

**Null rates**: `radio_call_sign` NULL for ~480 vessels (10%) — these are sub-15m boats without AIS. They cannot be tracked via AIS and will not appear in the fleet panel's live position column.

## CDC events

| event_type | event_subtype | Meaning | Typical volume |
|---|---|---|---|
| `new` | `vessel` | Vessel registered to this orgnr | ~2/week |
| `disappeared` | `vessel` | Vessel gone from this orgnr | ~2/week |
| `modified` | `vessel` | Operational fields changed (name, length, engine, callsign) | ~5/week |
| `modified` | `vessel_admin` | Only admin fields changed (municipality on reform dates) | rare |

**Vessel transfers**: a sale produces `disappeared` on seller's orgnr + `new` on buyer's orgnr, same `vessel_id`, same date. Join on `vessel_id` and `detected_time` to reconstruct transfers. ~1-2 transfers/week across the fleet.

## Admission rules (applied before diffing)

1. **Person-only vessels dropped** — no orgnr available (~15% of raw)
2. **Multi-owner vessels** — keep primary owner only (highest ownership %)
3. **Duplicate hulls** — same (registrationMark, radioCallSign) under different vessel_ids → keep most complete

All drops logged at `parsed/v1/drops/{date}.jsonl` with reason codes.

## Join keys

| To join with | Key | Coverage |
|---|---|---|
| Fangstdata (catch) | `radio_call_sign = fangstdata.radiokallesignal_seddel` | 90% of fangstdata rows (10% = foreign vessels) |
| AIS positions | `radio_call_sign → NSR.callsign → mmsi` | Indirect, via kystverket-ais-parser |
| Finstat | `orgnr = finstat.OffentligNr` (zero-pad float to 9-char string) | ~60% of fishing orgnrs have finstat |
| Løsøre | `orgnr = losore.orgnr` | ~30% of fishing orgnrs have active liens |
| Integration ledger | `orgnr + data_source='fiskeridir_vessel'` | Admitted |

**Gotcha**: finstat `OffentligNr` is float64 in the source parquet. You must `LPAD(CAST(CAST(OffentligNr AS BIGINT) AS VARCHAR), 9, '0')` before joining. Missing this produces zero matches.

## GCS layout

```
gs://sondre_brreg_data/fiskeridir/
├── raw/{date}/vessels.jsonl.gz           ← from collector
├── parsed/v1/state/{date}.parquet        daily snapshot (admitted rows)
├── parsed/v1/drops/{date}.jsonl          admission drop log
├── parsed/v1/meta/{date}.json            run metadata
└── cdc/changelog/{date}.parquet          12-column unified events
```

## Downstream

→ kystverket-ais-parser (callsign→orgnr bridge)
→ barentswatch-ais-live (same bridge)
→ fleet_panel (snapshot joined to fangstdata + finstat)
→ integration-layer (changelog admitted as `fiskeridir_vessel`)
