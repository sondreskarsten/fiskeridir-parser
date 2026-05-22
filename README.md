# fiskeridir-parser

Change-data-capture parser for the Norwegian fishing vessel registry. Reads the daily raw vessel dump from fiskeridir-collector, compares it against the prior day's snapshot, and emits a 12-column unified changelog describing exactly what changed: new vessels, disappeared vessels, modified fields, and ownership transfers.

## What does it detect?

Every day this pipeline answers: **what changed in the Norwegian fishing fleet since yesterday?** The answer is a structured event stream where each row is a single observed change:

| Event type | What it means | Credit signal |
|---|---|---|
| `new` | Vessel registered to this orgnr for the first time | **Vessel purchase** — new asset on balance sheet, likely financed |
| `disappeared` | Vessel was on this orgnr yesterday, gone today | **Vessel sold, scrapped, or deregistered** — collateral reduction |
| `modified` (vessel) | Fields changed: name, length, GT, engine, call sign, build year | **Rebuild, re-engine, or rename** — CAPEX signal or identity change |
| `modified` (vessel_admin) | Only administrative fields changed (municipality code on reform dates) | Noise — municipality merger, not operational |

**Vessel buying and selling** is the most important signal. When a fishing company sells a trawler to another company, the registry shows:
1. `disappeared` on the seller's orgnr (vessel gone from their fleet)
2. `new` on the buyer's orgnr (vessel appears in their fleet)
3. Both events share the same `vessel_id` and occur on the same `detected_time`

By joining disappeared + new events on `vessel_id` and date, you reconstruct the full transfer chain. This is visible 6-18 months before the transaction appears in either company's financial statements.

## LUAS (Lowest Unit of Analysis)

One row = one (orgnr, vessel_id) pair. A company that owns 3 vessels produces 3 rows in the snapshot. When vessel_id 12345 moves from orgnr A to orgnr B, that's 2 changelog events (1 disappeared + 1 new), not 1.

**Key**: `(orgnr, vessel_id)` — compound because the same physical hull (vessel_id) can change owners (orgnr) over time.

## What's in each vessel record?

| Field | Example | Why it matters |
|---|---|---|
| `orgnr` | "979356749" | Corporate owner — the join key to financial statements, liens, and corporate events |
| `vessel_id` | "12345" | Fiskeridir's internal hull identifier — stable across ownership transfers |
| `name` | "SLAATTERØY" | Vessel name — changes on sale or rename |
| `length` | 69.7 | Length in meters — determines length group (regulatory category) |
| `build_year` | 2003 | Year built — proxy for vessel condition and remaining useful life |
| `rebuild_year` | 2015 | Year of last major rebuild — CAPEX indicator |
| `engine_power_kw` | 5400 | Engine power — capacity indicator |
| `engine_build_year` | 2015 | Engine vintage — maintenance cycle indicator |
| `radio_call_sign` | "LGWH" | Radio call sign — the bridge to AIS vessel tracking |
| `municipality_code` | "1201" | Registration municipality — geographic signal (Bergen = deep-sea fleet, Lofoten = coastal) |
| `tonnage_gt` | 2187 | Gross tonnage — physical capacity measure |
| `last_lat`, `last_lon` | 60.3, 5.1 | Last AIS position (enriched from BarentsWatch during parse) |

## Admission rules

Not every record from the API enters the snapshot. Three filters, applied in order:

1. **No person-only vessels.** If the API record has no company owner (only a person), there's no orgnr to key on. Out of universe. (~15% of raw records)
2. **No multi-owner vessels.** When the API returns multiple owners for one vessel_id, it's a data artifact (secondary legal registrations), not balance-sheet co-ownership. We keep the primary owner only.
3. **No duplicate hulls.** Rare: same (registrationMark, radioCallSign) under different vessel_ids. Keep the most-complete record.

All dropped records are logged with reasons at `parsed/v1/drops/{date}.jsonl`.

## Pattern A: Bulk-diff CDC

This is a Pattern A pipeline. The changelog is an INDEX — it tells you *which* fields changed, but old/new values live in the dated snapshots. To get "what was the vessel's length before and after the change on May 15?", join the changelog to `state/2025-05-14.parquet` (old) and `state/2025-05-15.parquet` (new).

## GCS layout

```
gs://sondre_brreg_data/fiskeridir/
├── raw/{date}/vessels.jsonl.gz          from fiskeridir-collector
├── parsed/v1/
│   ├── state/{date}.parquet             daily snapshot: all admitted vessels
│   ├── drops/{date}.jsonl               admission drop log
│   └── meta/{date}.json                 run metadata
└── cdc/
    └── changelog/{date}.parquet         12-column unified events
```

## Changelog schema (12-column unified)

Identical to the platform's integration-layer schema:

| Column | Type | Notes |
|---|---|---|
| `orgnr` | string | 9-digit org number |
| `document_id` | string | `{orgnr}_{vessel_id}` |
| `data_source` | string | Always `fiskeridir_vessel` |
| `event_type` | string | `new` / `modified` / `disappeared` |
| `event_subtype` | string | `vessel` / `vessel_admin` |
| `summary` | string | Human-readable: "New vessel SLAATTERØY (69.7m, 2003)" |
| `changed_fields` | string | JSON array: `["length","engine_power_kw"]` |
| `valid_time` | timestamp | When the change occurred (best estimate: snapshot date) |
| `detected_time` | timestamp | When CDC detected it |
| `details_json` | string | NULL (Pattern A — values in snapshots) |
| `source_run_mode` | string | `daily` / `bootstrap` |
| `run_id` | string | UUID per execution |

## Ledger admission

Admitted to the integration-layer unified ledger as `data_source='fiskeridir_vessel'`. Events appear alongside enheter, roller, doffin, and other CDC streams — queryable via a single `SELECT * FROM ledger WHERE orgnr = '979356749'`.

## Downstream consumers

→ **kystverket-ais-parser**: uses `radio_call_sign` from the snapshot to bridge AIS mmsi → orgnr
→ **fleet_panel**: joins snapshot to fangstdata and finstat on orgnr
→ **integration-layer**: admits changelog to the unified ledger
→ **barentswatch-ais-live**: uses callsign→orgnr bridge for live position enrichment
