"""Universe admission for the fiskeridir vessel LUAS.

LUAS: (orgnr, vessel_id). Admission rules, applied in order:

  1. Drop PERSON-only vessels. No COMPANY owner means no orgnr, out of universe.
  2. Drop multi-owner vessels. Asset ownership is 1:1 on the balance sheet;
     multiple "owners" in the API are data artifacts or secondary-party legal
     registrations, not balance-sheet co-ownership.
  3. Drop duplicate hulls. When the same (registrationMark, radioCallSign)
     appears under multiple vessel_id, keep the vessel_id with the most
     complete metadata and drop the rest.

All drops are logged with reason; raw JSONL is preserved unchanged.
"""

from collections import defaultdict


def admit(raw_vessels):
    """Return (admitted_rows, drop_log).

    admitted_rows: list of dicts, each guaranteed to have a unique (orgnr, vessel_id)
    drop_log:      list of {vessel_id, reason, detail}
    """
    drops = []

    # Pass 1: single COMPANY owner only
    stage1 = []
    for v in raw_vessels:
        owners = v.get("owners") or []
        if len(owners) == 0:
            drops.append({"vessel_id": v.get("id"), "reason": "no_owners", "detail": {}})
            continue
        if len(owners) > 1:
            drops.append({
                "vessel_id": v.get("id"),
                "reason": "multi_owner",
                "detail": {"n_owners": len(owners),
                           "owner_types": [o.get("entityType") for o in owners]},
            })
            continue
        owner = owners[0]
        if owner.get("entityType") != "COMPANY":
            drops.append({
                "vessel_id": v.get("id"),
                "reason": "non_company_owner",
                "detail": {"entityType": owner.get("entityType")},
            })
            continue
        oid = owner.get("id")
        if not oid or not str(oid).isdigit():
            drops.append({
                "vessel_id": v.get("id"),
                "reason": "invalid_orgnr",
                "detail": {"id": oid},
            })
            continue
        v["_orgnr"] = str(oid)
        stage1.append(v)

    # Pass 2: deduplicate by (registrationMark, radioCallSign)
    #   metadata completeness score: +1 for each non-null of buildYear, imoNumber, registrationDate
    hull_groups = defaultdict(list)
    for v in stage1:
        key = (v.get("registrationMark"), v.get("radioCallSign"))
        if key == (None, None):
            hull_groups[("_single", v.get("id"))].append(v)
        else:
            hull_groups[key].append(v)

    admitted = []
    for key, group in hull_groups.items():
        if len(group) == 1:
            admitted.append(group[0])
            continue
        scored = [(
            (1 if v.get("buildYear") else 0) +
            (1 if v.get("imoNumber") else 0) +
            (1 if v.get("registrationDate") else 0),
            v,
        ) for v in group]
        scored.sort(key=lambda x: -x[0])
        winner = scored[0][1]
        admitted.append(winner)
        for _, loser in scored[1:]:
            drops.append({
                "vessel_id": loser.get("id"),
                "reason": "duplicate_hull",
                "detail": {"registrationMark": key[0], "radioCallSign": key[1],
                           "kept_vessel_id": winner.get("id")},
            })

    return admitted, drops
