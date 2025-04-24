#!/usr/bin/env sh

# Quit on error
set -e
set -o pipefail

# This script will create symbolic links to the most recent version of the
# parquet databases that form CrocoLake's data lake.

yaml_file="config.yaml"

# Extract the `ln_path` for the CROCOLAKE group
crocolake_ln=$(yq '.CROCOLAKE.ln_path' "$yaml_file")

# Check if CROCOLAKE ln_path exists
if [ -z "$crocolake_ln" ]; then
  echo "CROCOLAKE ln_path not found in the YAML file."
  exit 1
fi

# Create the CROCOLAKE output directory if it doesn't exist
mkdir -p "$crocolake_ln"

# Extract all `outdir_pq` entries from the YAML file
yq '.[] | select(has("outdir_pq")) | .outdir_pq' "$yaml_file" | while read -r outdir_pq; do
  # Skip the CROCOLAKE outdir_pq itself
  if [ "$outdir_pq" != "$crocolake_ln" ]; then
    # Create a symbolic link in the CROCOLAKE directory
    ln -s "$outdir_pq" "$crocolake_ln"
    echo "Created symlink for $outdir_pq in $crocolake_ln"
  fi
done
