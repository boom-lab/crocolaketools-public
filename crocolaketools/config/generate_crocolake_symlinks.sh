#!/usr/bin/env bash

# Quit on error
set -e
set -o pipefail

# This script will create symbolic links to the most recent version of the
# parquet databases that form CrocoLake's data lake.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
yaml_file="${SCRIPT_DIR}""/config.yaml"
crocolake_variants=(
  "PHY"
  "BGC"
)
# If variants are provided, use those instead
if [ $# -gt 0 ]; then
    # Clear default array
    crocolake_variants=()

    # Add each command-line argument to the array
    for arg in "$@"; do
        crocolake_variants+=("$arg")
    done
fi

# Loop through each variant of target CrocoLake directories
for var in "${crocolake_variants[@]}"; do

    # Extract the `ln_path` for the CROCOLAKE group
    crocolake_ln=$(yq ".CROCOLAKE_"$var".ln_path" "$yaml_file")
    # Check if CROCOLAKE ln_path exists
    if [ -z "$crocolake_ln" ]; then
      echo "CROCOLAKE ln_path not found in the YAML file."
      exit 1
    fi
    crocolake_ln=$(echo "$crocolake_ln" | sed 's/^"//;s/"$//')
    crocolake_ln=$(realpath "${SCRIPT_DIR}/${crocolake_ln}")

    crocolake_out=$(yq ".CROCOLAKE_"$var".outdir_pq" "$yaml_file")
    crocolake_out=$(echo "$crocolake_out" | sed 's/^"//;s/"$//')
    crocolake_out=$(realpath "${SCRIPT_DIR}/${crocolake_out}")

    echo "Creating folder $crocolake_ln if it does not exist"
    mkdir -p $crocolake_ln

    # Extract all `outdir_pq` entries from the YAML file
    yq --arg var "$var" '.[] | select(has("outdir_pq") and .db_type == $var) | .outdir_pq' "$yaml_file" | while read -r outdir_pq; do
      # Skip the CROCOLAKE outdir_pq itself
      outdir_pq=$(echo "$outdir_pq" | sed 's/^"//;s/"$//')
      outdir_pq=$(realpath "${SCRIPT_DIR}/${outdir_pq}")
      echo "Processing $outdir_pq"
      # skip if it's the crocolake dir or the other db type (bgc/phy)
      if [ "$outdir_pq" != "$crocolake_out" ] && [[ "$outdir_pq" != *"ARGO-GDAC"* ]]; then
        # Create a symbolic link in the CROCOLAKE directory
        if [ -d "$outdir_pq" ]; then
          echo "Creating symlink for $outdir_pq in $crocolake_ln"
          ln -s $outdir_pq $crocolake_ln
        else
          echo "Destination directory $outdir_pq does not exist. Skipping symlink creation. This usually happens if you have not generated this parquet dataset first."
        fi
      fi
    done
done
