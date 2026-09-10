#!/usr/bin/env bash
#
# Resolve the CrocoLake configuration directory for the shell layer.
#
# Sourced, not executed. Sets CONFIG_DIR, applying the same rules as
# crocolaketools/config/config_paths.py:
#
#   - CROCOLAKE_CONFIG_DIR is required; there is no default.
#   - Unset, not a directory, or missing either datasets.yaml or
#     dask_cluster.yaml is a fatal error.
#
# Signals failure with `return`, not `exit`, so that sourcing it by hand does
# not close the shell. Source it as `. resolve_config_dir.sh || exit 1`.

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
    echo "resolve_config_dir.sh must be sourced, not executed." >&2
    exit 1
fi

if [ -z "${CROCOLAKE_CONFIG_DIR:-}" ]; then
    echo "CROCOLAKE_CONFIG_DIR is not set. It names the directory holding datasets.yaml" >&2
    echo "and dask_cluster.yaml, and it is required: the package ships templates" >&2
    echo "(*.example.yaml), not a loadable default. To run against the committed test" >&2
    echo "fixtures, point it at the repository's tests/config directory." >&2
    return 1
else
    if [ ! -d "$CROCOLAKE_CONFIG_DIR" ]; then
        echo "CROCOLAKE_CONFIG_DIR is set to '${CROCOLAKE_CONFIG_DIR}', which is not a directory." >&2
        echo "Point it at a directory containing datasets.yaml and dask_cluster.yaml." >&2
        return 1
    fi
    for _crocolake_required in datasets.yaml dask_cluster.yaml; do
        if [ ! -f "${CROCOLAKE_CONFIG_DIR}/${_crocolake_required}" ]; then
            echo "CROCOLAKE_CONFIG_DIR is set to '${CROCOLAKE_CONFIG_DIR}', which is missing ${_crocolake_required}." >&2
            echo "A configuration directory must provide datasets.yaml and dask_cluster.yaml;" >&2
            echo "mixing a site file with a packaged one is not supported." >&2
            return 1
        fi
    done
    CONFIG_DIR="$CROCOLAKE_CONFIG_DIR"
fi

# -m tolerates paths that do not exist yet; -s leaves the `current` symlink
# pointing at `current` rather than at today's snapshot.
CONFIG_DIR=$(realpath -m -s "$CONFIG_DIR")
unset _crocolake_required
