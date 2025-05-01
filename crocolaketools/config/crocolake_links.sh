#!/usr/bin/env sh

# Quit on error
set -e
set -o pipefail

# This script will create symbolic links to the most recent version of the
# parquet databases that form CrocoLake's data lake.

SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
yaml_file="${SCRIPT_DIR}""/config.yaml"

# Extract the `ln_path` for the CROCOLAKE group
crocolake_ln=$(yq '.CROCOLAKE_PHY.ln_path' "$yaml_file")
# Check if CROCOLAKE ln_path exists
if [ -z "$crocolake_ln" ]; then
  echo "CROCOLAKE ln_path not found in the YAML file."
  exit 1
fi
crocolake_ln=$(echo "$crocolake_ln" | sed 's/^"//;s/"$//')
crocolake_ln=$(realpath "${SCRIPT_DIR}/${crocolake_ln}")

crocolake_out=$(yq '.CROCOLAKE_PHY.outdir_pq' "$yaml_file")
crocolake_out=$(echo "$crocolake_out" | sed 's/^"//;s/"$//')
crocolake_out=$(realpath "${SCRIPT_DIR}/${crocolake_out}")

echo "Creating folder $crocolake_ln if it does not exist"
mkdir -p $crocolake_ln

# Extract all `outdir_pq` entries from the YAML file
yq '.[] | select(has("outdir_pq")) | .outdir_pq' "$yaml_file" | while read -r outdir_pq; do
  # Skip the CROCOLAKE outdir_pq itself
  outdir_pq=$(echo "$outdir_pq" | sed 's/^"//;s/"$//')
  outdir_pq=$(realpath "${SCRIPT_DIR}/${outdir_pq}")
  echo "Processing outdir_pq: $outdir_pq"
  if [ "$outdir_pq" != "$crocolake_out" ]; then
    # Create a symbolic link in the CROCOLAKE directory
    echo "Creating symlink for $outdir_pq in $crocolake_ln"
    ln -s $outdir_pq $crocolake_ln
  fi
done
