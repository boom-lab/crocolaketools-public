#!/usr/bin/env sh

# Merge the sub-databases of CrocoLake into a single dataset.
#
# Paths come from the CROCOLAKE_<variant> entry of datasets.yaml in the
# directory named by CROCOLAKE_CONFIG_DIR: ln_path is read, outdir_pq is
# written, fname_pq names the output files.
#
# Usage: merge.sh PHY|BGC

set -e

usage() {
    echo "usage: $(basename "$0") PHY|BGC" >&2
    exit 1
}

[ "$#" -eq 1 ] || usage

# upper-cased so the check matches merge_crocolake, which accepts either case
VARIANT=$(printf '%s' "$1" | tr '[:lower:]' '[:upper:]')
case "$VARIANT" in
    PHY|BGC) ;;
    *) echo "$(basename "$0"): variant must be PHY or BGC, got '$1'" >&2; usage ;;
esac

merge_crocolake -d "$VARIANT" --config
