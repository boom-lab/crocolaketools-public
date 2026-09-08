#!/usr/bin/env python3

## @file test_downloaderSaildrones.py
#
#
# @author Alieldin Alaa <alieldinalaa04@gmail.com>
#
## @date Thu 19 Mar 2026

##########################################################################
import os
from unittest.mock import patch
import pytest

from crocolaketools.downloader.downloaderSaildrones import DownloaderSaildrones, SAILDRONES_URLS
from crocolaketools.downloader.downloader import Downloader
##########################################################################

# Mock the base init to bypass config/yaml loading during testing
@pytest.fixture(autouse=True)
def mock_base_init():
    with patch.object(Downloader, '__init__', lambda self, config: None):
        yield

class TestDownloaderSaildronesInit:
    """Tests for DownloaderSaildrones class inheritance and init method"""

    def test_inherits_downloader(self):
        """DownloaderSaildrones is a subclass of Downloader."""
        assert issubclass(DownloaderSaildrones, Downloader)

    def test_init_parameters(self):
        dl = DownloaderSaildrones(overwrite=True)
        assert dl.overwrite is True


class TestSaildronesDownloadMethod:
    """Testing saildrones downloading logic sequence."""

    @patch.object(DownloaderSaildrones, "unzip_file")
    @patch.object(DownloaderSaildrones, "_download_file")
    def test_saildrones_download_overwrite(self, mock_download, mock_unzip, tmp_path):
        """Existing files are re-downloaded and extracted when overwrite=True."""
        dl = DownloaderSaildrones()
        dl.input_path = str(tmp_path)
        dl.overwrite = True

        url = SAILDRONES_URLS[0]
        zip_fname = os.path.basename(url)
        nc_fname = zip_fname.replace('.nc_.zip', '.nc')

        # stage the extracted .nc, which is what the skip check actually tests
        (tmp_path / nc_fname).touch()

        with patch("crocolaketools.downloader.downloaderSaildrones.SAILDRONES_URLS", [url]):
            dl.saildrones_download()

        expected_zip = os.path.join(str(tmp_path), zip_fname)
        mock_download.assert_called_once_with(url, expected_zip)
        # unzip_file must be mocked too: _download_file is a mock, so no zip is
        # written, and a real unzip_file would raise into saildrones_download's
        # broad `except Exception` and let the test pass over a failed run
        mock_unzip.assert_called_once_with(expected_zip)

        
    @patch.object(DownloaderSaildrones, "unzip_file")
    @patch.object(DownloaderSaildrones, "_download_file")
    def test_saildrones_download_skip_existing(self, mock_download, mock_unzip, tmp_path):
        """An already-extracted .nc is skipped when overwrite=False."""
        dl = DownloaderSaildrones()
        dl.input_path = str(tmp_path)
        dl.overwrite = False

        url = SAILDRONES_URLS[0]
        (tmp_path / os.path.basename(url).replace('.nc_.zip', '.nc')).touch()

        with patch("crocolaketools.downloader.downloaderSaildrones.SAILDRONES_URLS", [url]):
            dl.saildrones_download()

        mock_download.assert_not_called()
        mock_unzip.assert_not_called()

##########################################################################

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
