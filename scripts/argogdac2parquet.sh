#!/usr/bin/env bash

# Quit on error
set -e
set -o pipefail

CONFIG_DIR="../crocolaketools/config"
yaml_file="${CONFIG_DIR}""/config.yaml"
argo_variants=(
  "BGC"
)
#  "PHY"

# Loop through each variant of target CrocoLake directories
for var in "${argo_variants[@]}"; do

    IN_PATH=$(yq ".\"ARGO-GDAC_${var}\".input_path" "$yaml_file")
    IN_PATH=$(echo "$IN_PATH" | sed 's/^"//;s/"$//')
    IN_PATH=$(realpath "${CONFIG_DIR}/${IN_PATH}")

    OUT_PATH=$(yq ".\"ARGO-GDAC_${var}\".outdir_pq" "$yaml_file")
    OUT_PATH=$(echo "$OUT_PATH" | sed 's/^"//;s/"$//')
    OUT_PATH=$(realpath "${CONFIG_DIR}/${OUT_PATH}")

    echo "Processing $var"
    echo "Input path: $IN_PATH"
    echo "Output path: $OUT_PATH"

    ## executing code
    argogdac2parquet \
        --db "$var" \
        --db_parquet "$OUT_PATH" \
        --gdac_index "$IN_PATH" \
        --db_nc "$IN_PATH" \
        --download "false"

done
