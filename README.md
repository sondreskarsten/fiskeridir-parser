# fiskeridir-parser

Parser for the Norwegian Fiskeridirektoratet vessel registry. Single LUAS `(orgnr, vessel_id)` with Merkeregisteret attributes and last-observed AIS position.

## Universe admission

Applied in order; all drops retained in `drops/{date}.jsonl`:

1. **PERSON-only vessels** — no COMPANY owner means no orgnr path; out of universe.
2. **Multi-owner vessels** — asset ownership is 1:1 on balance sheet; multiple "owners" in the API are either data artifacts or secondary-party legal registrations, not balance-sheet co-ownership.
3. **Duplicate hulls** — when the same `(registrationMark, radioCallSign)` appears under multiple `vessel_id`, keep the vessel_id with the most complete metadata (`buildYear`, `imoNumber`, `registrationDate` score), drop the rest.

## Snapshot attributes

Registry (from Fartøyregisteret): `name, length, build_year, rebuild_year, engine_power_kw, engine_build_year, radio_call_sign, municipality_code, tonnage_gt, tonnage_type`.

Position (from BarentsWatch `/v1/latest/ais` at parse time, joined on `radio_call_sign`): `last_lat, last_lon, last_position_ts`. If a callsign is absent from today's AIS snapshot, prior values carry forward. "Same as yesterday = no movement" is the consuming analysis layer's semantics.

## Changelog

Unified 12-col. `data_source='fiskeridir_vessel'`. Events:

- `new` — `(orgnr, vessel_id)` appeared
- `disappeared` — `(orgnr, vessel_id)` gone
- `modified` with `event_subtype='vessel'` — real registry attribute change
- `modified` with `event_subtype='vessel_admin'` — administrative mutation (`municipality_code` on kommunereform dates; `tonnage_type` convention flip without `tonnage_gt` change)

Position-only changes (`last_lat/lon/ts`) do not fire `modified` events.

## GCS layout

```
gs://sondre_brreg_data/fiskeridir/
  raw/{date}/vessels.jsonl.gz        (from collector)
  raw/{date}/meta.json               (from collector)
  parsed/v1/state/{date}.parquet     snapshot: admitted rows
  parsed/v1/drops/{date}.jsonl       admission drop log
  parsed/v1/meta/{date}.json         run metadata
  cdc/changelog/{date}.parquet       unified-12col events
```

## Schedule

`40 7 * * *` Europe/Oslo, 10 minutes after the collector.
