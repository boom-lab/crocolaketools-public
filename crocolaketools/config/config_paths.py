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

#: Environment variable naming the directory to read configuration from.
CONFIG_DIR_ENV_VAR = "CROCOLAKE_CONFIG_DIR"

#: Files a configuration directory must provide. Both, or neither: a directory
#: holding only datasets.yaml would leave the cluster sizing to come from
#: somewhere else, which is never what the author of either file intended.
REQUIRED_CONFIG_FILES = ("datasets.yaml", "cluster.yaml")

class ConfigDirError(RuntimeError):
    """Raised when CROCOLAKE_CONFIG_DIR is missing or unusable.

    Always fatal. There is no default configuration to fall back to, so a run
    either names the configuration it wants or it stops.
    """

def get_packaged_config_path() -> Path:
    """Return the directory holding the packaged configuration templates."""
    return Path(importlib.resources.files("crocolaketools.config"))

def get_config_path() -> Path:
    """Return the directory that configuration files are resolved against.

    Always CROCOLAKE_CONFIG_DIR, which is required. See datasets.example.yaml.
    """
    raw = os.environ.get(CONFIG_DIR_ENV_VAR)
    if raw is None or raw.strip() == "":
        raise ConfigDirError(
            f"{CONFIG_DIR_ENV_VAR} is not set. It names the directory holding "
            f"{' and '.join(REQUIRED_CONFIG_FILES)}, and it is required: the "
            f"package ships templates (*.example.yaml), not a loadable default. "
            f"To run against the committed test fixtures, point it at the "
            f"repository's tests/config directory."
        )

    config_dir = Path(os.path.expanduser(raw.strip()))
    if not config_dir.is_dir():
        raise ConfigDirError(
            f"{CONFIG_DIR_ENV_VAR} is set to {raw!r}, which is not a directory. "
            f"Point it at a directory containing "
            f"{' and '.join(REQUIRED_CONFIG_FILES)}."
        )

    missing = [f for f in REQUIRED_CONFIG_FILES if not (config_dir / f).is_file()]
    if missing:
        raise ConfigDirError(
            f"{CONFIG_DIR_ENV_VAR} is set to {config_dir}, which is missing "
            f"{', '.join(missing)}. A configuration directory must provide "
            f"{' and '.join(REQUIRED_CONFIG_FILES)}; mixing a site file with a "
            f"packaged one is not supported."
        )
    return Path(os.path.abspath(config_dir))

def get_config_paths_file() -> Path:
    return get_config_path() / "datasets.yaml"

def get_config_paths_db_dict(db_name: str) -> dict[str, Any]:
    config_paths = get_config_paths_file()
    with open(config_paths) as f:
        config_db = yaml.safe_load(f)[db_name]
    return config_db

def get_config_paths_field(db_name: str, field: str) -> Path:
    config_db = get_config_paths_db_dict(db_name)
    return resolve_config_path(config_db[field])

def get_config_cluster_file() -> Path:
    return get_config_path() / "cluster.yaml"

def get_config_cluster_db_dict(
        db_name: str,
        config_file: Optional[Union[str, Path]] = None,
) -> dict[str, Any]:
    """Read db_name's cluster settings.

    config_file -- path to cluster.yaml
                   (default: the resolved config dir's cluster.yaml)
    """
    config_paths = config_file if config_file is not None else get_config_cluster_file()
    with open(config_paths) as f:
        config_db = yaml.safe_load(f)[db_name]
    return config_db

def get_config_cluster_field(db_name: str, field: str) -> Path:
    config_db = get_config_cluster_db_dict(db_name)
    return resolve_config_path(config_db[field])

def add_config_dir_argument(parser) -> None:
    """Add --config-dir to an argparse parser.

    An alternative to exporting CROCOLAKE_CONFIG_DIR, for running by hand.
    """
    parser.add_argument(
        "--config-dir",
        default=None,
        metavar="DIR",
        help=(
            f"Directory holding {' and '.join(REQUIRED_CONFIG_FILES)}. "
            f"Overrides ${CONFIG_DIR_ENV_VAR}, which is used when this is "
            f"not given."
        ),
    )

def apply_config_dir_argument(args) -> None:
    """Honour --config-dir, if the parser defined one and the user gave it.

    Call before reading any configuration. The value goes into the environment
    so that the flag and the variable share a single resolution path, and so
    that shell scripts invoked further down see the same directory. It is made
    absolute first, because those may run from a different directory.
    """
    config_dir = getattr(args, "config_dir", None)
    if config_dir:
        os.environ[CONFIG_DIR_ENV_VAR] = os.path.abspath(
            os.path.expanduser(str(config_dir))
        )

def resolve_config_path(value: Union[str, Path]) -> Path:
    """Resolve a path read from a config file against the config directory.

    An absolute value is returned as-is, so a site config can name paths
    outside the package; a relative one is joined to the directory the config
    was read from, which is what keeps the packaged demo paths working.

    Symlinks are left intact: published datasets are addressed through a
    `current` symlink, and dereferencing it would record whichever snapshot it
    points at today.
    """
    path = Path(value)
    if not path.is_absolute():
        path = get_config_path() / path
    return Path(os.path.abspath(path))
