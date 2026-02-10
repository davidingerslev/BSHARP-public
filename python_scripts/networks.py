import pandas as pd
from . import services, distinct_pathways


def mkAdjEdgeLists(dffp, nw_df, nodetypes):
    dpw_start_dt = distinct_pathways.dpw_start_dt
    known_apw = dffp.loc[
        dffp.svc_type.isin(services.adult_pathways_accom_svc_types)
        & (dffp.pl_end_dt < dpw_start_dt),
        "o_cli_id",
    ]
    known_others = dffp.loc[
        (
            (dffp.svc_type.isin(services.accom_svc_types))
            & (dffp.pl_end_dt < dpw_start_dt)
            & (~dffp.o_cli_id.isin(known_apw))
        ),
        "o_cli_id",
    ]
    extant = nw_df.groupby("o_cli_id").head(1)[lambda x: x.pl_start_dt < dpw_start_dt]
    entries = nw_df.groupby("o_cli_id").head(1)[lambda x: x.pl_start_dt >= dpw_start_dt]
    entries_new = entries[~entries.o_cli_id.isin(pd.concat([known_apw, known_others]))]
    entries_apw = entries[entries.o_cli_id.isin(known_apw)]
    entries_others = entries[entries.o_cli_id.isin(known_others)]
    final_exits = nw_df.groupby(["o_cli_id"]).tail(1)[
        lambda x: x.pl_end_dt > dpw_start_dt
    ]
    # For re-entries, the route_id changes for the same o_cli_id
    nw_df_prev = nw_df.shift(1)
    reentries = nw_df[
        (nw_df.o_cli_id == nw_df_prev.o_cli_id)
        & (nw_df.route_id != nw_df_prev.route_id)
        & (nw_df.pl_start_dt >= dpw_start_dt)
    ].assign(prev_rt_end_cat=nw_df_prev.rt_end_cat)
    # Moves are all rows except entries/reentries (the first row in each route)
    # and each row contains details from the previous row for the same person
    moves = nw_df.groupby(["o_cli_id", "route_id"]).tail(-1)[
        lambda x: x.pl_start_dt >= dpw_start_dt
    ]

    edges_df = {}
    for et in nodetypes:
        prev_et = "prev_" + et

        nodedfs = []
        # Add all existing people
        if not extant.empty:
            nodedfs.append(
                extant.assign(start="EXTANT").rename(columns={et: "end"})[
                    ["start", "end"]
                ]
            )
        # Add all entries
        if not entries_new.empty:
            nodedfs.append(
                entries_new.assign(start="ENTRY", end="New")[["start", "end"]]
            )
            nodedfs.append(
                entries_new.assign(start="New").rename(columns={et: "end"})[
                    ["start", "end"]
                ]
            )
        if not entries_apw.empty:
            nodedfs.append(
                entries_apw.assign(start="ENTRY", end="Known (adult pw)")[
                    ["start", "end"]
                ]
            )
            nodedfs.append(
                entries_apw.assign(start="Known (adult pw)").rename(
                    columns={et: "end"}
                )[["start", "end"]]
            )
        if not entries_others.empty:
            nodedfs.append(
                entries_others.assign(start="Known (other)").rename(
                    columns={et: "end"}
                )[["start", "end"]]
            )
            nodedfs.append(
                entries_others.assign(start="ENTRY", end="Known (other)")[
                    ["start", "end"]
                ]
            )
        # Add all exits
        if not final_exits.empty:
            nodedfs.append(
                final_exits.rename(columns={et: "start", "rt_end_cat": "end"})[
                    ["start", "end"]
                ]
            )
            nodedfs.append(
                final_exits.assign(end="END").rename(columns={"rt_end_cat": "start"})[
                    ["start", "end"]
                ]
            )
        # Add moves back from EXIT to ENTRY and then onwards for reentries
        if not reentries.empty:
            nodedfs.append(
                reentries.rename(
                    columns={"prev_" + et: "start", "prev_rt_end_cat": "end"}
                )[["start", "end"]]
            )
            nodedfs.append(
                reentries.rename(columns={"prev_rt_end_cat": "start"}).assign(
                    end="RETURN"
                )[["start", "end"]]
            )
            # reentries.assign(start="_RETURN", end="_"+reentries.prev_rt_end_cat)[["start", "end"]],
            # reentries.assign(start="_"+reentries.prev_rt_end_cat, end=reentries[et])[["start", "end"]],
            nodedfs.append(
                reentries.assign(start="_RETURN").rename(columns={et: "end"})[
                    ["start", "end"]
                ]
            )
        # Add all other moves except where the move was within the same service
        if not moves.empty:
            nodedfs.append(
                moves[
                    (~moves.prev_svc_id.isnull()) & (moves.svc_id != moves.prev_svc_id)
                ].rename(columns={prev_et: "start", et: "end"})[["start", "end"]]
            )

        if len(nodedfs) > 0:
            edges_df[et] = pd.concat(nodedfs)
        else:
            edges_df[et] = pd.DataFrame(columns=["start", "end"])

    adj_lists = {}
    for et in nodetypes:
        adj = (
            edges_df[et]
            .fillna("OTHER")
            .groupby(["start", "end"], dropna=False)
            .size()
            .to_frame()
        )
        adj_list = adj.reset_index().rename(
            columns={"start": "source", "end": "target", 0: "weight"}
        )
        # adj_list = adj_list[adj_list.weight >= min_threshold]
        adj_list["n"] = adj_list["weight"]
        adj_lists[et] = adj_list

    edge_lists = {}
    for et in nodetypes:
        edge_lists[et] = adj_lists[et].copy()
        edge_lists[et].weight = (
            adj_lists[et].groupby("source")["weight"].transform(lambda x: x / x.sum())
        )

    return adj_lists, edge_lists
