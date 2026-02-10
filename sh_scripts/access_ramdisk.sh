#!/bin/bash
JOB_FILE="/tmp/empty_ramdisk_at.job"

# From https://serverfault.com/questions/50585/whats-the-best-way-to-check-if-a-volume-is-mounted-in-a-bash-script
isPathMounted() {
  findmnt --target "$1" >/dev/null
}
# From https://stackoverflow.com/questions/8455991/elegant-way-for-verbose-mode-in-scripts
log() {
  #if [[ $_VERBOSE -eq 1 ]]; then
  echo "$@"
  #fi
}

# If -v is used as an argument, then print progress
while getopts "v" OPTION; do
  case $OPTION in
  v)
    _VERBOSE=1
    ;;
  esac
done

# Stop and exit on first error
set -e

# Cancel previous job if it exists
if [ -f "$JOB_FILE" ]; then
  JOB_ID=$(cat "$JOB_FILE")
  atrm "$JOB_ID" 2>/dev/null
  log "Canceled previous deletion job: $JOB_ID"
fi

# Mount VPN-accessed data drive
if isPathMounted "/mnt/x"; then
  log "/mnt/x is already mounted"
else
  sudo /bin/mount -t drvfs X: /mnt/x &&
    log "Mounted /mnt/x"
fi

# Mount ramdisk
if isPathMounted "/mnt/ramdisk"; then
  log "/mnt/ramdisk is already mounted"
else
  sudo /bin/mount -t tmpfs tmpfs -o size=150M,noswap,uid=1000,gid=1000,mode=0700 /mnt/ramdisk &&
    log "Mounted /mnt/ramdisk"
fi

# Schedule new deletion in 1 hour
FILE_TO_DELETE="/mnt/ramdisk/*"
JOB_ID=$(echo "rm -rf $FILE_TO_DELETE" | at now + 1 hour 2>&1 | grep -oE '[0-9]+' | head -1)
echo "$JOB_ID" >"$JOB_FILE"
log "Scheduled deletion of $FILE_TO_DELETE in 1 hour (job $JOB_ID)"
