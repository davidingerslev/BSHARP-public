import pandas as pd
from . import services, setup, routes


dpw_start_dt = pd.Timestamp("2017-10-28")
dpw_end_dt = pd.Timestamp("2025-04-30")
nodetypes = ["svc_id", "svc_typelvl", "svc_lvls"]
capacities = {
    "M L1": 110,
    "M L2": 29,
    "M L3": 40,
    "M L4": 161,
    "M/F L1": 85,
    "M/F L2": 38,
    "M/F L3": 21,
    "M/F L4": 81,
    "F L1": 21,
    "F L2": 27,
    "F L3": 30,
    "F L4": 70,
    "SU L1": 89,
    "SU L2": 51,
}


def get_sorted_end_cats(dpw_pls):
    sorted_end_cats = (
        dpw_pls.rt_end_cat.value_counts()
        .reset_index()
        .assign(to=lambda x: x.rt_end_cat.str.startswith("To "))
    )
    sorted_end_cats = (
        pd.concat(
            [
                sorted_end_cats[lambda x: x.to].sort_values(by="count"),
                sorted_end_cats[lambda x: ~x.to].sort_values(
                    by="count", ascending=False
                ),
            ]
        )
        .reset_index()
        .rt_end_cat
    )
    sorted_end_cats = dict((v, k) for k, v in sorted_end_cats.items())
    return sorted_end_cats


stable_offsets = {
    "90d": pd.DateOffset(days=90),
    "6m": pd.DateOffset(months=6),
    "15m": pd.DateOffset(months=15),
    "18m": pd.DateOffset(months=18),
    "2y": pd.DateOffset(years=2),
    "5y": pd.DateOffset(years=5),
}
stable_reasons = [
    "To friends/family",
    "To private rented",
    "To social housing",
    "To care/hosp.",
    "To sheltered",
    "To external supported",
]


def get_distinct_pathways_routes(dffp):
    dpw_pls = dffp[
        dffp.svc_type.isin(services.distinct_pathways_accom_svc_types)
        & (dffp.pl_start_dt <= dpw_end_dt)
        & (
            (dffp.pl_end_dt >= dpw_start_dt)  # Ended since DPW started
            | dffp.pl_end_dt.isna()  # Still open
        )
    ]
    # Reset all the added columns
    dpw_pls = setup.drop_added_cols(dpw_pls)
    dpw_pls = setup.sort_values_and_add_cols(dpw_pls)

    # Add a unique identifier for each route (contiguous journey through supported housing services)
    dpw_pls = setup.add_routes(dpw_pls)

    end_categories = routes.get_end_cats_map()
    dpw_pls["rt_end_cat"] = (
        dpw_pls.pl_end_reason.map(end_categories)
        .fillna("[Not ended or invalid end reason]")
        .where(dpw_pls.vac_id.isin(dpw_pls.groupby("route_id").tail(1).vac_id))
        .groupby([dpw_pls.o_cli_id, dpw_pls.route_id])
        .bfill()
    )
    end_planned = routes.get_is_planned_map()
    dpw_pls["rt_end_planned"] = dpw_pls.pl_end_reason.map(end_planned).where(
        dpw_pls.vac_id.isin(dpw_pls.groupby("route_id").tail(1).vac_id)
        .groupby([dpw_pls.o_cli_id, dpw_pls.route_id])
        .bfill()
    )
    dpw_pls = setup.drop_added_cols(dpw_pls)
    dpw_pls = setup.sort_values_and_add_cols(dpw_pls)

    next_pl_start_dt = dpw_pls.pl_start_dt.shift(-1).where(
        dpw_pls.o_cli_id == dpw_pls.o_cli_id.shift(-1)
    )
    route_end_vacids = (
        dpw_pls.groupby(["o_cli_id", "route_id"])
        .tail(1)[lambda t: ~t.pl_end_dt.isna()]
        .vac_id
    )
    last_route_end_vacids = (
        dpw_pls.groupby(["o_cli_id"]).tail(1)[lambda t: ~t.pl_end_dt.isna()].vac_id
    )
    last_route_ends = dpw_pls.vac_id.isin(last_route_end_vacids)
    notlast_route_ends = dpw_pls.vac_id.isin(route_end_vacids) & ~dpw_pls.vac_id.isin(
        last_route_end_vacids
    )
    stable_route_ends = dpw_pls.vac_id.isin(route_end_vacids) & dpw_pls.rt_end_cat.isin(
        stable_reasons
    )
    last_stable_route_ends = stable_route_ends & dpw_pls.vac_id.isin(
        last_route_end_vacids
    )
    notlast_stable_route_ends = stable_route_ends & ~dpw_pls.vac_id.isin(
        last_route_end_vacids
    )
    for period in stable_offsets:
        # ends of routes where the end reason is in the list of stable reasons
        # and either the next route after starts after the offset, or it's the most
        # recent route and it ends on a date greater than the offest before 30/4/25
        col = f"after_rt_ret_within_{period}"
        dpw_pls[col] = pd.NA
        dpw_pls.loc[notlast_route_ends, col] = (
            (dpw_pls.pl_end_dt + stable_offsets[period]) < next_pl_start_dt
        ).map({True: "No", False: "Yes"})
        dpw_pls.loc[last_route_ends, col] = (
            (dpw_pls.pl_end_dt + stable_offsets[period]) < dpw_end_dt
        ).map({True: "No", False: "Not yet..."})
        dpw_pls[col] = (
            dpw_pls[col].groupby([dpw_pls.o_cli_id, dpw_pls.route_id]).bfill()
        )

    shorttypelvls_base = {
        "Mixed Pathway": "M/F",
        "Male Only Pathway": "M",
        "Female Only Pathway": "F",
        "Substance Misuse Pathway": "SU",
    }
    shorttypelvls = {}
    for typelvl in shorttypelvls_base:
        for i in range(1, 5):
            shorttypelvls[f"{typelvl} L{i}"] = f"{shorttypelvls_base[typelvl]} L{i}"

    dpw_pls["svc_typelvl"] = dpw_pls.svc_type_short.astype(
        "string"
    ) + dpw_pls.pathway_level.str.replace(r"^(\d)", r" L\1", regex=True).fillna("")
    dpw_pls["prev_svc_typelvl"] = dpw_pls.prev_svc_type_short.astype(
        "string"
    ) + dpw_pls.prev_pathway_level.str.replace(r"^(\d)", r" L\1", regex=True).fillna("")
    for col in ["svc_typelvl", "prev_svc_typelvl"]:
        dpw_pls[col] = dpw_pls[col].map(shorttypelvls)

    dpw_pls["svc_lvls"] = dpw_pls.svc_apwlvl.astype("string")
    dpw_pls["prev_svc_lvls"] = dpw_pls.prev_svc_apwlvl.astype("string")
    for typ in ["Emergency L1", "High Support L2", "Medium Support L3"]:
        dpw_pls.loc[dpw_pls.svc_type_short == typ, "svc_lvls"] = "EHM_" + typ[-1:]
        dpw_pls.loc[dpw_pls.prev_svc_type_short == typ, "prev_svc_lvls"] = (
            "EHM_" + typ[-1:]
        )

    return dpw_pls
