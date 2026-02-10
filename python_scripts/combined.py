import pandas as pd
from pathlib import Path
from collections import namedtuple
from . import (
    vacancies,
    clients,
    services,
    trusted_assessments,
    placements,
    routes,
    setup,
)
from threading import Lock

pd.options.mode.copy_on_write = True
_dfs_to_save_to_ramdisk = [
    "vac",
    "vac_moved",
    "cli",
    "svc",
    "tr_a",
]


# Get dataframes from cache
def get_dataframes(basePath="./Original-CSVs", verbose=True):
    df, dffp = setup.setup(basepath=basePath, verbose=verbose)
    return df


def _get_combined_dfs(df_dict: dict, verbose=False):
    if verbose:
        print("Combining datasets...")
    # Combine datasets
    df_dict["all"] = (
        pd.merge(df_dict["vac"], df_dict["cli"], how="outer", on="cli_id")
        .merge(df_dict["svc"], how="left", on="svc_id")
        .merge(df_dict["tr_a"].reset_index(), how="left", on="cli_id")
    )
    # Get list of cli_ids who have used adult pathways accommodation
    adultpathway_users = (
        df_dict["all"]
        .loc[
            (
                (df_dict["all"]["o_cli_id"].notna())
                & (df_dict["all"]["vac_id"].notna())
                & (
                    df_dict["all"]["svc_type"].isin(
                        services.adult_pathways_accom_svc_types
                    )
                )
            ),
            "o_cli_id",
        ]
        .drop_duplicates()
    )
    # Create dataset of all service uses for people who have used adult pathways accommodation
    df_dict["f_service_use"] = df_dict["all"][
        (df_dict["all"]["o_cli_id"].isin(adultpathway_users))
        & (df_dict["all"]["vac_id"].notna())
    ]
    # Create dataset of accommodation service uses for people who have used adult pathways accommodation
    df_dict["f_placements"] = df_dict["all"][
        (df_dict["all"]["o_cli_id"].isin(adultpathway_users))
        & (df_dict["all"]["vac_id"].notna())
        & (df_dict["all"]["svc_type"].isin(services.accom_svc_types))
    ]
    dffp = placements.get_placements(df_dict["f_placements"])
    dffp = placements.eliminate_overlaps(dffp)
    dffp = placements.reduce_gaps(dffp)
    dffp = routes.add_routes(dffp)
    df_dict["f_placements_corrected"] = dffp
    return df_dict


# Get dataframes
def _get_dataframes_real(basePath="./Original-CSVs", verbose=True, do_cleaning=True):
    # Check that basePath exists
    dir = Path(basePath)
    if dir.exists() and dir.is_dir():
        print("Base path exists: " + str(dir.absolute()))
    else:
        raise FileNotFoundError(f"Base path does not exist: '{basePath}'")

    # Load base datasets
    df_moved_vac, df_vac = vacancies._get_vacancies_real(basePath, verbose, do_cleaning)
    df_cli = clients._get_clients_real(basePath, verbose, do_cleaning)
    df_svc = services._get_services_real(basePath, verbose, do_cleaning)
    df_tr_a = trusted_assessments._get_trusted_assessments_real(
        basePath, verbose, do_cleaning
    )

    # Create dict
    df_dict = {
        "vac": df_vac,
        "vac_moved": df_moved_vac,
        "cli": df_cli,
        "svc": df_svc,
        "tr_a": df_tr_a,
    }

    # Add combined dfs to dict
    df_dict = _get_combined_dfs(df_dict, verbose)

    if verbose:
        print("Done. Returning named tuple with pandas dataframes:")
        print("  vac: Vacancies")
        print("  cli: People who have used services")
        print("  svc: Services")
        print("  tr_a: Trusted Assessment forms")
        print(
            "  all_merged: All datasets, vac/cli outer joined, others left joined on cli_id."
        )
        print(
            "  f_serviceuse: Filter of merged datasets: ",
            "    all placements for people who had at least one placement in the adult pathway",
            sep="\n",
        )
        print(
            "  f_placements: Filter of merged datasets: ",
            "    accommodation placements for people who had at least one placement in the adult pathway",
            sep="\n",
        )
        print(
            "  f_placements_corrected: Filter of merged datasets, with corrections: ",
            "    accommodation placements for people who had at least one placement in the adult pathway",
            sep="\n",
        )
        print(
            "  vac_moved: Vacancies moved out of vac: ",
            "    acommodation placements that took place during other placements.",
            sep="\n",
        )
        print("-----")
        print("Example usage: df.f_placements.svc_type.value_counts()")

    return namedtuple("Struct", df_dict)(**df_dict)
