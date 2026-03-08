import pytest

import global_data_pipeline.storage.local as local_store
from global_data_pipeline.storage.state import PipelineState


# ---------------------------------------------------------------------------
# PipelineState
# ---------------------------------------------------------------------------


def test_state_starts_empty(tmp_path):
    state = PipelineState(tmp_path, "test_source")
    assert state.last_run is None
    assert state.indicator_count() == 0


def test_state_update_and_reload(tmp_path):
    state = PipelineState(tmp_path, "test_source")
    state.update_indicator("IND1", last_updated="2024-01-01", row_count=100, checksum="sha256:abc")
    state.save()

    reloaded = PipelineState(tmp_path, "test_source")
    assert reloaded.indicator_count() == 1
    assert reloaded.get_indicator("IND1")["row_count"] == 100


def test_state_last_run_set_on_save(tmp_path):
    state = PipelineState(tmp_path, "test_source")
    assert state.last_run is None
    state.save()
    assert state.last_run is not None


def test_should_skip_when_unchanged(tmp_path):
    state = PipelineState(tmp_path, "test_source")
    state.update_indicator("IND1", last_updated="2024-01-01")
    assert state.should_skip("IND1", "2024-01-01") is True


def test_should_not_skip_when_updated(tmp_path):
    state = PipelineState(tmp_path, "test_source")
    state.update_indicator("IND1", last_updated="2024-01-01")
    assert state.should_skip("IND1", "2024-02-01") is False


def test_should_not_skip_unknown_indicator(tmp_path):
    state = PipelineState(tmp_path, "test_source")
    assert state.should_skip("NEW_IND", "2024-01-01") is False


def test_should_not_skip_when_no_last_updated(tmp_path):
    state = PipelineState(tmp_path, "test_source")
    state.update_indicator("IND1", last_updated="2024-01-01")
    # current_last_updated=None means we can't determine freshness
    assert state.should_skip("IND1", None) is False


def test_compute_checksum():
    cs = PipelineState.compute_checksum(b"hello")
    assert cs.startswith("sha256:")
    assert len(cs) == len("sha256:") + 64


# ---------------------------------------------------------------------------
# Local storage
# ---------------------------------------------------------------------------


def test_write_and_read_indicator(tmp_data_dir, sample_df):
    datasets_dir = tmp_data_dir / "datasets"
    local_store.write_indicator(datasets_dir, "test_source", "IND1", sample_df)
    result = local_store.read_indicator(datasets_dir, "test_source", "IND1")
    assert result is not None
    assert len(result) == len(sample_df)


def test_read_missing_indicator_returns_none(tmp_data_dir):
    datasets_dir = tmp_data_dir / "datasets"
    assert local_store.read_indicator(datasets_dir, "test_source", "MISSING") is None


def test_read_source_combines_files(tmp_data_dir, sample_df):
    datasets_dir = tmp_data_dir / "datasets"
    local_store.write_indicator(datasets_dir, "test_source", "IND1", sample_df)
    local_store.write_indicator(datasets_dir, "test_source", "IND2", sample_df)
    combined = local_store.read_source(datasets_dir, "test_source")
    assert len(combined) == len(sample_df) * 2


def test_read_source_empty_returns_empty_df(tmp_data_dir):
    datasets_dir = tmp_data_dir / "datasets"
    result = local_store.read_source(datasets_dir, "nonexistent")
    assert result.empty


def test_source_stats(tmp_data_dir, sample_df):
    datasets_dir = tmp_data_dir / "datasets"
    local_store.write_indicator(datasets_dir, "test_source", "IND1", sample_df)
    stats = local_store.source_stats(datasets_dir, "test_source")
    assert stats["files"] == 1
    assert stats["total_rows"] == len(sample_df)
    assert stats["size_mb"] > 0


def test_indicator_code_with_slash_sanitised(tmp_data_dir, sample_df):
    """Indicator codes with slashes (common in IMF) must be stored safely."""
    datasets_dir = tmp_data_dir / "datasets"
    local_store.write_indicator(datasets_dir, "imf", "BOP/A.US", sample_df)
    result = local_store.read_indicator(datasets_dir, "imf", "BOP/A.US")
    assert result is not None
