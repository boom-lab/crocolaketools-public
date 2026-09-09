#!/usr/bin/env python3

## @file config_paths.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Fri 04 Oct 2024

##########################################################################
import importlib.resources
import os
from pathlib import Path
from typing import Any, Optional, Union

import yaml
##########################################################################

def get_config_path() -> Path:
    """Return the directory that configuration files are resolved against."""
    return Path(importlib.resources.files("crocolaketools.config"))

def get_config_paths_file() -> Path:
    return get_config_path() / "config.yaml"

def get_config_paths_db_dict(db_name: str) -> dict[str, Any]:
    config_paths = get_config_paths_file()
    with open(config_paths) as f:
        config_db = yaml.safe_load(f)[db_name]
    return config_db

def get_config_paths_field(db_name: str, field: str) -> Path:
    config_db = get_config_paths_db_dict(db_name)
    return resolve_config_path(config_db[field])

def get_config_cluster_file() -> Path:
    return get_config_path() / "config_cluster.yaml"

def get_config_cluster_db_dict(
        db_name: str,
        config_file: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Read db_name's cluster settings.

    config_file -- path to config_cluster.yaml
                   (default: crocolaketools/config/config_cluster.yaml)
    """
    config_paths = config_file if config_file is not None else get_config_cluster_file()
    with open(config_paths) as f:
        config_db = yaml.safe_load(f)[db_name]
    return config_db

def get_config_cluster_field(db_name: str, field: str) -> Path:
    config_db = get_config_cluster_db_dict(db_name)
    return resolve_config_path(config_db[field])

def resolve_config_path(value: Union[str, Path]) -> Path:
    """Resolve a path read from a config file against the config directory.

    An absolute value is returned as-is, so a site config can name paths
    outside the package; a relative one is joined to the directory the config
    was read from, which is what keeps the packaged demo paths working.

    Normalised with os.path.abspath rather than Path.resolve(): symlinks are
    deliberately left intact, because published datasets are addressed through
    a `current` symlink and resolving it would record the snapshot it happens
    to point at today.
    """
    path = Path(value)
    if not path.is_absolute():
        path = get_config_path() / path
    return Path(os.path.abspath(path))
