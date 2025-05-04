#!/usr/bin/env sh

IN_DB_NAME="0003_PHY_CROCOLAKE-QC"
IN_PATH="/vortexfs1/share/boom/users/enrico.milanese/myDatabases/${IN_DB_NAME}/current/"
OUT_DB_NAME="0007_PHY_CROCOLAKE-QC-MERGED"
OUT_PATH="/vortexfs1/share/boom/users/enrico.milanese/myDatabases/${OUT_DB_NAME}/current/"
mkdir -p "$OUT_PATH"

merge_crocolake -d PHY -i "$IN_PATH" -o "$OUT_PATH" -f "$OUT_DB_NAME"
