from . import combined, placements, routes
from . import helper
from . import ramdisk
import pandas as pd
from pathlib import Path
import pickle
import os
import subprocess


def sort_values_and_add_cols(*args, **kwargs):
    return placements.sort_values_and_add_cols(*args, **kwargs)


def drop_added_cols(*args, **kwargs):
    return placements.drop_added_cols(*args, **kwargs)


def get_placements(*args, **kwargs):
    return placements.get_placements(*args, **kwargs)


def add_routes(*args, **kwargs):
    return routes.add_routes(*args, **kwargs)


def reduce_gaps(*args, **kwargs):
    return placements.reduce_gaps(*args, **kwargs)


def eliminate_overlaps(*args, **kwargs):
    return placements.eliminate_overlaps(*args, **kwargs)


def get_df_dffp(basepath: str, do_cleaning=True):
    df = combined._get_dataframes_real(basepath, do_cleaning)

    # Placements is based on filtered combined dataframe
    dffp = df.f_placements_corrected

    return df, dffp


def get_dffp_uncorrected(df: pd.DataFrame):
    return get_placements(df.f_placements)


#################################################################
# Load objects from RAM disk, if they've been saved, otherwise
# generate them from the CSV files and then save save them to
# the RAM disk so loading is near-instantaneous for other loads
#################################################################
def setup(
    ramdisk_dir: str = "/mnt/ramdisk/",
    basepath: str = "/mnt/x/Original-CSVs/",
    reload=False,
    verbose=True,
):
    if (
        ramdisk.is_mounted(ramdisk_dir)
        and ramdisk.files_exist(ramdisk_dir)
        and not reload
    ):
        ramdisk.create_emptying_job(verbose=False, cancel_previous=True)
        helper.log(
            "Objects already loaded to RAM disk; deletion timer reset.", verbose=verbose
        )
        df = ramdisk.load_df(ramdisk_dir)
        return df, df.f_placements_corrected
    else:
        if not reload:
            helper.log("Files not loaded to RAM disk.", verbose=verbose)
            helper.log(
                "Mounting drives and resetting deletion timer...", verbose=verbose
            )
        else:
            helper.log(
                "Mounting drives (if needed) and resetting deletion timer...",
                verbose=verbose,
            )
        mount_drives_or_update_deletion_timer(ramdisk_dir=ramdisk_dir, verbose=verbose)
        helper.log("Loading from CSV...", verbose=verbose)
        df, dffp = get_df_dffp(basepath)
        ramdisk.save_df(df, ramdisk_dir=ramdisk_dir)
        return df, dffp


def save_df_dffp_to_ramdisk(
    df: pd.DataFrame | None = None,
    dffp: pd.DataFrame | None = None,
    ramdisk_dir: str = "/mnt/ramdisk/",
    verbose=True,
):
    df_dict_path = Path(ramdisk_dir) / "df_dict.p"
    dffp_path = Path(ramdisk_dir) / "dffp.p"

    oldmask = os.umask(0o077)
    if df is not None:
        df_dict = df._asdict()
        pickle.dump(df_dict, open(df_dict_path, "wb"))
    if dffp is not None:
        pickle.dump(dffp, open(dffp_path, "wb"))
    os.umask(oldmask)


def mount_drives_or_update_deletion_timer(
    ramdisk_dir: str = "/mnt/ramdisk/",
    original_csvs_dir: str = "/mnt/x/",
    original_csvs_mount_cmd: str = "sudo /bin/mount -t drvfs X: /mnt/x",
    verbose=True,
):
    ramdisk.cancel_emptying_job(verbose)

    # Mount /mnt/x if needed
    csv_mountpoint = original_csvs_dir
    result = subprocess.run(["mountpoint", "-q", csv_mountpoint])
    if result.returncode != 0:
        helper.log(csv_mountpoint, "is not mounted.", verbose=verbose)
        helper.log("Checking VPN connection", verbose=verbose)
        result = subprocess.run(
            "ipconfig.exe | grep -c 'UoB VPN' | tr -d '\n'",
            shell=True,
            check=True,
            capture_output=True,
        )
        n_matches = int(result.stdout.decode("utf8"))
        if n_matches == 1:
            helper.log("VPN is connected", verbose=verbose)
        else:
            raise ConnectionError(
                "VPN connection could not be detected. Ensure the VPN client is running."
            )

        subprocess.run(original_csvs_mount_cmd, shell=True, check=True)
        helper.log(f"Mounted {csv_mountpoint}", verbose=verbose)
    else:
        helper.log(csv_mountpoint, "is already mounted", verbose=verbose)

    ramdisk.mount_drive_if_needed(ramdisk_dir=ramdisk_dir, verbose=verbose)
    ramdisk.create_emptying_job(verbose)


def attach_debugger():
    import debugpy

    debugpy.listen(5678)
    debugpy.wait_for_client()
