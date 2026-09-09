#!/usr/bin/env python3

## @file test_config_paths.py
#
# Tests for the config/path resolution layer.
#
## @date Mon 07 Sep 2026

##########################################################################
import os
from pathlib import Path

import pytest

import crocolaketools.config.config_paths as cfgp
from crocolaketools.downloader.downloader import Downloader

TEST_CONFIG_CLUSTER_FILE = Path(__file__).parent / "config_cluster_tests.yaml"

# a db/db_type pair that exists in the packaged config.yaml
DB = "GLODAP"
DB_TYPE = "PHY"
DB_KEY = f"{DB}_{DB_TYPE}"
##########################################################################


class TestConfigPaths:
    """Tests for crocolaketools/config/config_paths.py"""

    def test_config_path_is_the_packaged_config_dir(self):
        """get_config_path() points at the installed config package."""
        base = Path(str(cfgp.get_config_path()))
        assert base.is_dir()
        assert (base / "config.yaml").is_file()

    def test_config_paths_file(self):
        """get_config_paths_file() resolves to config.yaml inside it."""
        path = Path(str(cfgp.get_config_paths_file()))
        assert path.name == "config.yaml"
        assert path.is_file()

    def test_db_dict_has_required_keys(self):
        """A db block carries at least db, db_type and input_path."""
        cfg = cfgp.get_config_paths_db_dict(DB_KEY)
        assert cfg["db"] == DB
        assert cfg["db_type"] == DB_TYPE
        assert "input_path" in cfg

    def test_db_dict_unknown_key_raises(self):
        """An unknown db key raises KeyError rather than returning a default."""
        with pytest.raises(KeyError):
            cfgp.get_config_paths_db_dict("NOT_A_DB_PHY")

    def test_field_resolves_against_the_config_dir(self):
        """get_config_paths_field() joins the relative value onto the config dir.

        The result is absolute and normalised: config.yaml's values are written
        relative to the config dir ("../../tests/fixtures/..."), and the ".."
        segments are collapsed so consumers can compare paths by equality.
        """
        raw = cfgp.get_config_paths_db_dict(DB_KEY)["input_path"]
        resolved = cfgp.get_config_paths_field(DB_KEY, "input_path")
        assert resolved == Path(os.path.abspath(cfgp.get_config_path() / raw))
        assert resolved.is_absolute()
        assert ".." not in resolved.parts

    def test_absolute_field_is_left_alone(self):
        """An absolute config value is not joined onto the config dir.

        This is what lets a site config name paths outside the package.
        """
        assert cfgp.resolve_config_path("/srv/crocolake/phy") == Path("/srv/crocolake/phy")

    def test_symlinks_are_not_resolved(self, tmp_path):
        """Resolution normalises but does not follow symlinks.

        Published datasets are addressed through a `current` symlink; resolving
        it would record whichever snapshot it points at today.
        """
        real = tmp_path / "snapshots" / "2026-09-09"
        real.mkdir(parents=True)
        link = tmp_path / "current"
        link.symlink_to(real)
        assert cfgp.resolve_config_path(link) == link

    def test_cluster_file(self):
        """get_config_cluster_file() resolves to config_cluster.yaml."""
        path = Path(str(cfgp.get_config_cluster_file()))
        assert path.name == "config_cluster.yaml"
        assert path.is_file()

    def test_cluster_db_dict_default_file(self):
        """With no config_file, the packaged config_cluster.yaml is read."""
        cfg = cfgp.get_config_cluster_db_dict("GLODAP")
        assert "n_workers" in cfg
        assert "threads_per_worker" in cfg

    def test_cluster_db_dict_explicit_file(self):
        """An explicit config_file overrides the packaged one."""
        cfg = cfgp.get_config_cluster_db_dict(
            "TESTS", config_file=TEST_CONFIG_CLUSTER_FILE
        )
        assert cfg["n_workers"] == 1
        assert cfg["processes"] is True


class TestDownloaderConfigResolution:
    """Tests for Downloader.__init__ -- config merge and path normalisation."""

    def test_no_config_raises(self):
        """A missing config is rejected explicitly."""
        with pytest.raises(ValueError, match="No config argument"):
            Downloader()

    def test_unknown_db_raises(self):
        """An unknown db/db_type pair fails on the config.yaml lookup."""
        with pytest.raises(KeyError):
            Downloader(config={"db": "NOT_A_DB", "db_type": "PHY"})

    def test_input_path_is_an_absolute_path_object(self, tmp_path):
        """input_path is a resolved, absolute Path -- never a string."""
        target = tmp_path / "original"
        d = Downloader(config={
            "db": DB,
            "db_type": DB_TYPE,
            "input_path": str(target),
        })
        assert isinstance(d.input_path, Path)
        assert d.input_path.is_absolute()
        assert d.input_path == target

    def test_input_path_directory_is_created(self, tmp_path):
        """The destination directory is created if absent."""
        target = tmp_path / "does" / "not" / "exist"
        assert not target.exists()
        Downloader(config={
            "db": DB,
            "db_type": DB_TYPE,
            "input_path": str(target),
        })
        assert target.is_dir()

    def test_db_and_db_type_are_stored(self, tmp_path):
        """db and db_type land on the instance, db_type upper-cased.

        A lower-case db_type also trips the mismatch warning, because the
        constructor compares the user's raw value against config.yaml's
        after upper-casing only its own copy.
        """
        with pytest.warns(UserWarning, match="not matching at key db_type"):
            d = Downloader(config={
                "db": DB,
                "db_type": DB_TYPE.lower(),
                "input_path": str(tmp_path),
            })
        assert d.db == DB
        assert d.db_type == DB_TYPE

    def test_absent_keys_are_filled_from_config_yaml(self, tmp_path):
        """Keys the user omits are read from the db's config.yaml block."""
        disk = cfgp.get_config_paths_db_dict(DB_KEY)
        config = {"db": DB, "db_type": DB_TYPE, "input_path": str(tmp_path)}
        d = Downloader(config=config)

        # GLODAP_PHY declares overwrite in config.yaml; the user did not
        assert "overwrite" in disk
        assert d.overwrite == disk["overwrite"]
        # the merge writes back into the caller's dict -- a side effect worth
        # pinning, since it makes the constructor unsafe to call twice with
        # the same dict
        assert config["fname_pq"] == disk["fname_pq"]

    def test_user_values_win_over_config_yaml(self, tmp_path):
        """A key the user supplies is not overwritten by the disk value."""
        disk = cfgp.get_config_paths_db_dict(DB_KEY)
        assert disk["overwrite"] is True, "fixture assumes config.yaml sets True"
        d = Downloader(config={
            "db": DB,
            "db_type": DB_TYPE,
            "input_path": str(tmp_path),
            "overwrite": False,
        })
        assert d.overwrite is False

    def test_download_option_defaults(self, tmp_path):
        """Options absent from both user config and config.yaml take defaults."""
        disk = cfgp.get_config_paths_db_dict(DB_KEY)
        assert "num_threads" not in disk and "dryrun" not in disk
        d = Downloader(config={
            "db": DB,
            "db_type": DB_TYPE,
            "input_path": str(tmp_path),
        })
        assert d.num_threads == 4
        assert d.dryrun is False


##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
