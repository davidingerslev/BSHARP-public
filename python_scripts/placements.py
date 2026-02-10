import pandas as pd

sort_order = [
    "o_cli_id",
    "pl_end_dt",
    "pl_start_dt",
    "vac_filled_dt",
    "vac_end_dt",
    "notification_dt",
]


def get_placements(df_passed: pd.DataFrame, sort_order: str | list[str] or None = None):
    dffp = df_passed.copy()
    # Shorten column names
    dffp.columns = dffp.columns.str.replace("placement", "pl").str.replace(
        "duration", "dur"
    )
    # Add columns for placement start/end dates (these will be updated)
    dffp["pl_start_dt"] = dffp.vac_filled_dt
    dffp["pl_end_dt"] = dffp.moved_out_dt.fillna(dffp.vac_end_dt)
    # Sort values and add more columns
    if sort_order is not None:
        dffp = sort_values_and_add_cols(dffp, sort_order)
    else:
        dffp = sort_values_and_add_cols(dffp)
    return dffp


def sort_values_and_add_cols(dffp: pd.DataFrame):
    # Re-sort the placements
    dffp = dffp.sort_values(sort_order)
    # Add a duration field
    dffp["dur"] = dffp.pl_end_dt - dffp.pl_start_dt
    # Add number of moves (including within services)
    dffp["moves_all"] = dffp.groupby("o_cli_id")["o_cli_id"].transform("count") - 1
    present_cols_for_prev = dffp.columns[dffp.columns.isin(cols_for_prev)]
    # Create temporary shifted dataframe
    dffp_prev_same_oclid = (
        dffp[present_cols_for_prev]
        .shift(1)
        .where(dffp.o_cli_id == dffp.shift(1).o_cli_id)
        .rename((lambda x: "prev_" + x), axis="columns")
    )
    # Combine the placement dataframe with the dataframe of previous placements
    dffp = dffp.join(dffp_prev_same_oclid)
    # Add gap field, excluding cases where the previous row was for a different person
    dffp["gap"] = dffp.pl_start_dt - dffp.prev_pl_end_dt
    return dffp


def drop_added_cols(dffp: pd.DataFrame):
    # Drop the columns we're about to recreate
    cols_to_drop = (
        dffp.columns[dffp.columns.isin(["gap", "dur", "moves_all"])].tolist()
        + dffp.columns[dffp.columns.str.startswith("prev_")].tolist()
    )
    dffp = dffp.drop(columns=cols_to_drop)
    return dffp


def eliminate_overlaps(dffp: pd.DataFrame):
    # Un-backdate internal transfers
    dffp_prev = dffp.shift(1)
    rows_to_correct = (
        (dffp.o_cli_id == dffp_prev.o_cli_id)
        & (dffp_prev.pl_end_reason == "INTERNAL TRANSFER")
        & (dffp.gap.dt.days < 0)
    )
    # Eliminate overlaps by making second (newer) placement start at the end date of the first placement
    dffp.loc[rows_to_correct, ["pl_start_dt", "vac_filled_dt"]] = dffp_prev.loc[
        rows_to_correct, "pl_end_dt"
    ]
    dffp["correction__unbackdated"] = (
        rows_to_correct  # Adds a True/False column identifying cases where the gap was removed
    )
    dffp = dffp.sort_values(sort_order)  # Re-sort columns
    # Update gap
    dffp.loc[(dffp.o_cli_id == dffp_prev.o_cli_id), "gap"] = (
        dffp.pl_start_dt - dffp_prev.pl_end_dt
    )

    # Unbackdate moves that were within the same service
    dffp_prev = dffp.shift(1)  # Update previous
    rows_to_correct = (
        (dffp.o_cli_id == dffp_prev.o_cli_id)
        & (dffp_prev.svc_id == dffp.svc_id)
        & (dffp.gap.dt.days < 0)
    )
    # Eliminate overlaps by making second (newer) placement start at the end date of the first placement
    dffp.loc[rows_to_correct, ["pl_start_dt", "vac_filled_dt"]] = dffp_prev.loc[
        rows_to_correct, "pl_end_dt"
    ]
    dffp["correction__unbackdated"] = (
        dffp["correction__unbackdated"]
        | rows_to_correct  # Add these backdate corrections
    )
    dffp = dffp.sort_values(sort_order)  # Re-sort columns
    # Update gap
    dffp.loc[(dffp.o_cli_id == dffp_prev.o_cli_id), "gap"] = (
        dffp.pl_start_dt - dffp_prev.pl_end_dt
    )

    # Eliminate remaining overlaps
    dffp_next = dffp.shift(-1)
    rows_to_correct = (dffp.o_cli_id == dffp_next.o_cli_id) & (
        dffp_next.gap.dt.days < 0
    )  # where the next placement has a negative gap
    # Eliminate remaining overlaps by making first (older) placement end at the start date of the new placement
    dffp["pl_end_dt"] = dffp["pl_end_dt"].mask(
        cond=rows_to_correct,
        other=dffp_next.pl_start_dt,
    )  # replace with the next placement start date
    dffp["moved_out_dt"] = dffp["pl_end_dt"]
    dffp.loc[dffp.gap.dt.days < 0, "gap"] = pd.Timedelta(
        days=0
    )  # set negative gaps to 0
    dffp["correction__overlap_removed"] = (
        rows_to_correct  # Adds a True/False column identifying cases where the gap was removed
    )
    dffp = drop_added_cols(dffp)
    dffp = sort_values_and_add_cols(dffp)

    return dffp


def reduce_gaps(dffp: pd.DataFrame):
    # Eliminate gaps between services using different thresholds depending on the
    # reason for leaving the previous placement.
    gap_comparison_dict = {
        "Moved into Supported Housing (Planned)": 8,
        "Moved within Supported Housing (Same Pathway)": 31,
        "Other (Unplanned)": 1,
        "Moved into HSR Accom _ Lower level (Planned)": 8,
        "INTERNAL TRANSFER": 8,
        "Other (Planned)": 1,
        "Moved into HSR Accom _ High level (Planned)": 8,
        "Moved within Supported Housing (Different Pathway)": 20,
        "Moved into HSR Accom _ Same level (Planned)": 14,
        "Moved to Substance Misuse Pathway (Planned)": 31,
        "Moved into HSR Accom _ High level (Unplanned)": 31,
        "(Pathway 4 Only) Moved to Level 1, 2 or 3 Supported Housing (Planned)": 31,
        "Moved into HSR Accom _ Same level (Unplanned)": 31,
        "(Pathway 4 Only) Moved to Level 4 Supported Housing (Planned)": 31,
    }
    gap_comparison_series = dffp.prev_pl_end_reason.map(gap_comparison_dict)
    rows_to_correct = (
        (~gap_comparison_series.isna())
        & (dffp.gap.dt.days > 0)
        & (dffp.gap.dt.days <= gap_comparison_series)
    )
    dffp.loc[rows_to_correct, ["pl_start_dt", "vac_filled_dt"]] = dffp.loc[
        rows_to_correct, "prev_pl_end_dt"
    ]
    dffp["correction__gap_removed"] = (
        rows_to_correct  # Adds a True/False column identifying cases where the gap was removed
    )
    dffp = drop_added_cols(dffp)
    dffp = sort_values_and_add_cols(dffp)
    return dffp


cols_for_prev = [
    "o_cli_id",
    "vac_id",
    "pl_start_dt",
    "pl_end_dt",
    "vac_address_outward_code",
    "vac_end_dt",
    "ref_id",
    "filled_by_ref_interview_dt",
    "filled_by_ref_nomination_dt",
    "ref_agency",
    "filled_by_ref_dt",
    "ref_type",
    "filled_by_ref_result_dt",
    "ref_result_set_on",
    "vac_filled_dt",
    "filled_dt_entered_on",
    "filled_type",
    "moveon_dt",
    "moved_out_dt",
    "notification_dt",
    "overnight_pl",
    "pl_end_reason",
    "move_on_ref_agency",
    "svc_contract_id",
    "shared",
    "vac_start_dt",
    "start_dt_entered_on",
    "vac_close_dt_set_on",
    "vac_days",
    "vac_end_time",
    "filled_by_ref__result_dt",
    "pl_dur_days",
    "pl_dur_nights",
    "svc_id",
    "cli_id",
    "ref_agency_short",
    "ref_agency_short_padded",
    "svc_type_short",
    "pathway_level",
    "svc_type",
    "svc_capacity",
    "clients_accepted",
    "minimum_age",
    "maximum_age",
    "primary_cli_grp",
    "secondary_cli_grp",
    "svc_support_level",
    "provider_id",
    "OAB",
    "svc_type_short_padded",
    "svc_apwlvl",
]
