from . import helper
from . import combined
import subprocess
import re
from pathlib import Path
import pickle
import os
from collections import namedtuple

_default_ramdisk_dir = "/mnt/ramdisk/"
_default_ramdisk_mount_cmd = "sudo /bin/mount -t tmpfs tmpfs -o size=200M,noswap,uid=1000,gid=1000,mode=0700 /mnt/ramdisk"
_df_filename = "df_dict.p"


def mount_drive_if_needed(
    ramdisk_dir: str = _default_ramdisk_dir,
    ramdisk_mount_cmd: str = _default_ramdisk_mount_cmd,
    verbose=True,
):
    if is_mounted(ramdisk_dir=ramdisk_dir):
        helper.log(f"{ramdisk_dir} is already mounted", verbose=verbose)
    else:
        subprocess.run(ramdisk_mount_cmd, shell=True, check=True)
        helper.log(f"Mounted {ramdisk_dir}", verbose=verbose)


def save_df(df: namedtuple, ramdisk_dir: str = _default_ramdisk_dir):
    df_dict_path = Path(ramdisk_dir) / _df_filename
    df_dict = df._asdict()
    df_to_save = dict((k, df_dict[k]) for k in combined._dfs_to_save_to_ramdisk)
    oldmask = os.umask(0o077)
    pickle.dump(df_to_save, open(df_dict_path, "wb"))
    os.umask(oldmask)


def load_df(ramdisk_dir: str = _default_ramdisk_dir):
    df_dict_path = Path(ramdisk_dir) / _df_filename
    df_dict = pickle.load(open(df_dict_path, "rb"))
    df_dict = combined._get_combined_dfs(df_dict)
    df = namedtuple("Struct", df_dict)(**df_dict)
    return df


def files_exist(ramdisk_dir: str = _default_ramdisk_dir):
    df_dict_path = Path(ramdisk_dir) / _df_filename
    return df_dict_path.exists() and df_dict_path.is_file()


def is_mounted(ramdisk_dir: str = _default_ramdisk_dir):
    result = subprocess.run(["mountpoint", "-q", ramdisk_dir])
    return result.returncode == 0


def cancel_emptying_job(verbose: bool = True):
    # Cancel the previous job if it exists
    _job_file = Path("/tmp/empty_ramdisk_at.job")
    if _job_file.exists() and _job_file.is_file():
        job_id = _job_file.read_text()
        subprocess.run(["atrm", job_id], check=True)
        _job_file.unlink()
        helper.log("Canceled previous ramdisk deletion job:", job_id, verbose=verbose)


def create_emptying_job(verbose: bool = True, cancel_previous: bool = False):
    if cancel_previous:
        cancel_emptying_job(verbose)
    # Schedule new deletion
    _job_file = Path("/tmp/empty_ramdisk_at.job")
    files_to_delete = "/mnt/ramdisk/*"
    result = subprocess.run(
        ["at", "now", "+", "3", "hours"],
        input=f"rm -rf {files_to_delete} {_job_file}".encode("utf8"),
        capture_output=True,
    )
    job_id = re.search(r"[0-9]+", result.stderr.decode("utf8")).group()
    _job_file.write_text(job_id)
    helper.log(
        f"Scheduled deletion of {files_to_delete} in 3 hours (job {job_id})",
        verbose=verbose,
    )
