from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# huggingface.publish_source
# ---------------------------------------------------------------------------


def test_publish_unknown_source_skips(tmp_path, caplog):
    from global_data_pipeline.publish.huggingface import publish_source

    with caplog.at_level("WARNING"):
        publish_source("unknown_source", tmp_path, token="hf_test")

    assert "No HF repo configured" in caplog.text


def test_publish_no_data_skips(tmp_path, caplog):
    from global_data_pipeline.publish.huggingface import publish_source

    with caplog.at_level("WARNING"):
        publish_source("world_bank", tmp_path, token="hf_test")

    assert "No data found" in caplog.text


def test_publish_calls_upload_folder(tmp_path, sample_df):
    import global_data_pipeline.storage.local as local_store
    from global_data_pipeline.publish.huggingface import publish_source

    # Write a real parquet file so the source_path check passes
    datasets_dir = tmp_path / "datasets"
    datasets_dir.mkdir()
    local_store.write_indicator(datasets_dir, "world_bank", "NY.GDP.MKTP.CD", sample_df)

    mock_api = MagicMock()
    with patch("global_data_pipeline.publish.huggingface.HfApi", return_value=mock_api):
        publish_source("world_bank", datasets_dir, token="hf_test")

    mock_api.create_repo.assert_called_once()
    mock_api.upload_folder.assert_called_once()


# ---------------------------------------------------------------------------
# database.load_source
# ---------------------------------------------------------------------------


def test_load_source_no_data_warns(tmp_path, caplog):
    from global_data_pipeline.publish.database import load_source

    with patch("global_data_pipeline.publish.database.create_engine") as mock_engine:
        mock_conn = MagicMock()
        mock_engine.return_value.begin.return_value.__enter__ = lambda *a: mock_conn
        mock_engine.return_value.begin.return_value.__exit__ = MagicMock(return_value=False)

        with caplog.at_level("WARNING"):
            load_source("world_bank", tmp_path / "datasets", "postgresql://fake/db")

    assert "No data found" in caplog.text
