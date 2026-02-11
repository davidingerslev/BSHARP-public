import pandas as pd


def add_routes(dffp: pd.DataFrame):
    # Add a unique identifier for each route (contiguous journey through supported housing services)
    # Create a column for route_id, filled with NA values
    dffp["route_id"] = pd.Series(pd.NA, dtype="Int64")
    # Assign an incrementing route_id for each placement with a non-zero (or NaT) gap
    qfilter = dffp.gap.dt.days != 0
    df_route_starts = dffp[qfilter]
    dffp.loc[qfilter, "route_id"] = range(1, len(df_route_starts) + 1)
    # Fill the route_id forwards to all NA values (i.e. gap==0)
    dffp["route_id"] = dffp["route_id"].ffill()
    return dffp


end_reasons_map = {
    # { pl_end_reason: { (rt_end_cat, is_planned) } }
    "Abandoned (Unplanned)": ("Abandoned", "Unplanned"),
    "(FS) Unknown / lost contact": ("Abandoned", "Unplanned"),
    "(FS) Abandoned tenancy": ("Abandoned", "Unplanned"),
    "Custody - Current Offence (Unplanned)": ("Custody", "Unplanned"),
    "Taken into Custody (Unplanned)": ("Custody", "Unplanned"),
    "Custody - Breach of Prior Order (Unplanned)": ("Custody", "Unplanned"),
    "(FS) Taken into custody": ("Custody", "Unplanned"),
    "Custody - Court Hearing/Arrest Warrant for Prior Offence (Planned)": (
        "Custody",
        "Unplanned",
    ),
    "Death (Unplanned)": ("Died", "Unplanned"),
    "(FS) Died": ("Died", "Unplanned"),
    "Evicted (Unplanned)": ("Evicted", "Unplanned"),
    "(FS) Evicted": ("Evicted", "Unplanned"),
    "Hospital, Care Home or Hospice (Planned)": ("To care/hospital", "Planned"),
    "Psychiatric Hospital (Planned)": ("To care/hospital", "Planned"),
    "Moved into Sheltered Housing (Planned)": ("To sheltered", "Planned"),
    "Moved into Care Home (Planned)": ("To care/hospital", "Planned"),
    "Moved into Sheltered Housing (Unplanned)": ("To sheltered", "Unplanned"),
    "(FS) Entered a long-stay hosp": ("To care/hospital", "Planned"),
    "Hospital, Care Home or Hospice (Unplanned)": ("To care/hospital", "Unplanned"),
    "Psychiatric Hospital (Unplanned)": ("To care/hospital", "Unplanned"),
    "Staying with Friends or Family (Planned)": ("To friends/family", "Planned"),
    "Staying with Friends (Planned)": ("To friends/family", "Planned"),
    "(FS) Moved in with family or relatives (planned)": (
        "To friends/family",
        "Planned",
    ),
    "(FS) Moved in with friends (planned)": ("To friends/family", "Planned"),
    "(A&A) Moved in with Family & Friends (Long-term)": (
        "To friends/family",
        "Planned",
    ),
    "Staying with Friends or Family (Unplanned)": ("To friends/family", "Unplanned"),
    "Staying with Friends (Unplanned)": ("To friends/family", "Unplanned"),
    "Moved into Supported Housing (Planned)": ("To external supported", "Planned"),
    "Non-HSR Supported Accommodation (Planned)": ("To external supported", "Planned"),
    "Non-HSR Substance Misuse Accommodation (Planned)": (
        "To external supported",
        "Planned",
    ),
    "Non-HSR Supported Accommodation (Unplanned)": (
        "To external supported",
        "Unplanned",
    ),
    "Moved into Supported Housing (Unplanned)": ("To external supported", "Unplanned"),
    "Renting Privately (Planned)": ("To private rented", "Planned"),
    "(FS) Moved into Private Self Contained (planned)": (
        "To private rented",
        "Planned",
    ),
    "(FS) Moved into Private Shared Accom (planned)": ("To private rented", "Planned"),
    "Renting Privately (Unplanned)": ("To private rented", "Unplanned"),
    "Moved to Local Authority Tenancy (Planned)": ("To social housing", "Planned"),
    "BCC Social Tenancy via PMOS (Planned)": ("To social housing", "Planned"),
    "Moved to RSL Tenancy (Planned)": ("To social housing", "Planned"),
    "BCC Social Tenancy NOT via PMOS (Planned)": ("To social housing", "Planned"),
    "RSL Social Tenancy via PMOS (Planned)": ("To social housing", "Planned"),
    "RSL Social Tenancy NOT via PMOS (Planned)": ("To social housing", "Planned"),
    "Moved to RSL Tenancy (Unplanned)": ("To social housing", "Unplanned"),
    "Moved to Local Authority Tenancy (Unplanned)": (
        "To social housing",
        "Unplanned",
    ),
    "(FS) Moved into RSL (planned)": ("To social housing", "Planned"),
    "Other (Planned)": ("Other", "Planned"),
    "Moved into Accomm as an Owner Occupier (Planned)": ("Other", "Planned"),
    "Move into B&B (Planned)": ("Other", "Planned"),
    "Other (Unplanned)": ("Other", "Unplanned"),
    "Move into B&B (Unplanned)": ("Other", "Unplanned"),
    "Sleeping Rough (Unplanned)": ("Other", "Unplanned"),
    "(SSTS) BCC Emergency Accommodation (FOR SSTS USE ONLY)": ("Other", "Unplanned"),
    "Moved within Supported Housing (Same Pathway)": (
        "Missing data/error",
        "Unknown",
    ),
    "INTERNAL TRANSFER": ("Missing data/error", "Unknown"),
    "Moved within Supported Housing (Different Pathway)": (
        "Missing data/error",
        "Unknown",
    ),
    "Moved into HSR Accom _ Lower level (Planned)": (
        "Missing data/error",
        "Unknown",
    ),
    "Moved into HSR Accom _ High level (Planned)": (
        "Missing data/error",
        "Unknown",
    ),
    "Moved into HSR Accom _ Same level (Planned)": (
        "Missing data/error",
        "Unknown",
    ),
    "Moved to Substance Misuse Pathway (Planned)": (
        "Missing data/error",
        "Unknown",
    ),
    "(Pathway 4 Only) Moved to Level 4 Supported Housing (Planned)": (
        "Missing data/error",
        "Unknown",
    ),
    "(Pathway 4 Only) Moved to Level 1, 2 or 3 Supported Housing (Planned)": (
        "Missing data/error",
        "Unknown",
    ),
    "Moved into HSR Accom _ High level (Unplanned)": (
        "Missing data/error",
        "Unknown",
    ),
    "Moved into HSR Accom _ Same level (Unplanned)": (
        "Missing data/error",
        "Unknown",
    ),
    "Returned to Previous Home (Unplanned)": ("Returned to previous home", "Unplanned"),
    "Returned to Previous Home (Planned)": ("Returned to previous home", "Planned"),
}

grouped_cats = {
    # { rt_end_cat: { is_planned: {pl_end_reason, ...} } }
    "Abandoned": {
        "Unplanned": {
            "Abandoned (Unplanned)",
            "(FS) Unknown / lost contact",
            "(FS) Abandoned tenancy",
        }
    },
    "Custody": {
        "Unplanned": {
            "Custody - Current Offence (Unplanned)",
            "Taken into Custody (Unplanned)",
            "Custody - Breach of Prior Order (Unplanned)",
            "(FS) Taken into custody",
            "Custody - Court Hearing/Arrest Warrant for Prior Offence (Planned)",
        }
    },
    "Died": {"Unplanned": {"Death (Unplanned)", "(FS) Died"}},
    "Evicted": {"Unplanned": {"Evicted (Unplanned)", "(FS) Evicted"}},
    "Missing data/error": {
        "Unknown": {
            "Moved within Supported Housing (Same Pathway)",
            "INTERNAL TRANSFER",
            "Moved within Supported Housing (Different Pathway)",
            "Moved into HSR Accom _ Lower level (Planned)",
            "Moved into HSR Accom _ High level (Planned)",
            "Moved into HSR Accom _ Same level (Planned)",
            "Moved to Substance Misuse Pathway (Planned)",
            "(Pathway 4 Only) Moved to Level 4 Supported Housing (Planned)",
            "(Pathway 4 Only) Moved to Level 1, 2 or 3 Supported Housing (Planned)",
            "Moved into HSR Accom _ High level (Unplanned)",
            "Moved into HSR Accom _ Same level (Unplanned)",
        }
    },
    "Other": {
        "Unplanned": {
            "Other (Unplanned)",
            "Move into B&B (Unplanned)",
            "Sleeping Rough (Unplanned)",
            "(SSTS) BCC Emergency Accommodation (FOR SSTS USE ONLY)",
        },
        "Planned": {
            "Other (Planned)",
            "Moved into Accomm as an Owner Occupier (Planned)",
            "Move into B&B (Planned)",
        },
    },
    "Returned to previous home": {
        "Unplanned": {"Returned to Previous Home (Unplanned)"},
        "Planned": {"Returned to Previous Home (Planned)"},
    },
    "To care/hospital": {
        "Unplanned": {
            "Hospital, Care Home or Hospice (Unplanned)",
            "Psychiatric Hospital (Unplanned)",
        },
        "Planned": {
            "Hospital, Care Home or Hospice (Planned)",
            "Psychiatric Hospital (Planned)",
            "Moved into Care Home (Planned)",
            "(FS) Entered a long-stay hosp",
        },
    },
    "To external supported": {
        "Unplanned": {
            "Non-HSR Supported Accommodation (Unplanned)",
            "Moved into Supported Housing (Unplanned)",
        },
        "Planned": {
            "Moved into Supported Housing (Planned)",
            "Non-HSR Supported Accommodation (Planned)",
            "Non-HSR Substance Misuse Accommodation (Planned)",
        },
    },
    "To friends/family": {
        "Unplanned": {
            "Staying with Friends or Family (Unplanned)",
            "Staying with Friends (Unplanned)",
        },
        "Planned": {
            "Staying with Friends or Family (Planned)",
            "Staying with Friends (Planned)",
            "(FS) Moved in with family or relatives (planned)",
            "(FS) Moved in with friends (planned)",
            "(A&A) Moved in with Family & Friends (Long-term)",
        },
    },
    "To private rented": {
        "Unplanned": {"Renting Privately (Unplanned)"},
        "Planned": {
            "Renting Privately (Planned)",
            "(FS) Moved into Private Self Contained (planned)",
            "(FS) Moved into Private Shared Accom (planned)",
        },
    },
    "To sheltered": {
        "Unplanned": {"Moved into Sheltered Housing (Unplanned)"},
        "Planned": {"Moved into Sheltered Housing (Planned)"},
    },
    "To social housing": {
        "Unplanned": {
            "Moved to RSL Tenancy (Unplanned)",
            "Moved to Local Authority Tenancy (Unplanned)",
        },
        "Planned": {
            "Moved to Local Authority Tenancy (Planned)",
            "BCC Social Tenancy via PMOS (Planned)",
            "Moved to RSL Tenancy (Planned)",
            "BCC Social Tenancy NOT via PMOS (Planned)",
            "RSL Social Tenancy via PMOS (Planned)",
            "RSL Social Tenancy NOT via PMOS (Planned)",
            "(FS) Moved into RSL (planned)",
        },
    },
}


def map_to_grouped_cats(end_reasons_map):
    # { pl_end_reason: { (rt_end_cat, is_planned) } }
    grouped = {}
    for reason in end_reasons_map:
        end_cat, is_planned = end_reasons_map[reason]
        if end_cat not in grouped:
            grouped[end_cat] = {}
            grouped[end_cat][is_planned] = {reason}
        elif is_planned not in grouped[end_cat]:
            grouped[end_cat][is_planned] = {reason}
        else:
            grouped[end_cat][is_planned].add(reason)
    return grouped


def grouped_cats_to_map(grouped_cats):
    # { rt_end_cat: { is_planned: {pl_end_reason, ...} } }
    map = {}
    for end_cat, dct in grouped_cats.items():
        for is_planned, st in dct.items():
            for reason in st:
                if reason in map:
                    raise ValueError(f"duplicate reason '{reason}' found")
                else:
                    map[reason] = (end_cat, is_planned)
    return map


def get_end_cats_map():
    cats_map = {k: v for k, (v, _) in end_reasons_map.items()}
    return cats_map


def get_is_planned_map():
    planned_map = {k: v for k, (_, v) in end_reasons_map.items()}
    return planned_map
