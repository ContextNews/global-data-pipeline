"""Publish local Parquet files to Hugging Face Hub.

Uses upload_folder() rather than push_to_hub() to avoid loading large
datasets into memory. One HF dataset repo per source.
"""

from pathlib import Path

from huggingface_hub import HfApi

from global_data_pipeline.logging import get_logger

log = get_logger("publish.huggingface")

# HF namespace is configured here; update once the HF org/user is decided.
_NAMESPACE = "ContextNews"

_REPO_IDS: dict[str, str] = {
    "world_bank": f"{_NAMESPACE}/world-bank-indicators",
    "imf": f"{_NAMESPACE}/imf-data",
    "un_sdg": f"{_NAMESPACE}/un-sdg-indicators",
}


def publish_source(source_name: str, datasets_dir: Path, token: str) -> None:
    """Upload all Parquet files for a source to its Hugging Face dataset repo."""
    repo_id = _REPO_IDS.get(source_name)
    if repo_id is None:
        log.warning("No HF repo configured for source '%s' — skipping", source_name)
        return

    source_path = datasets_dir / source_name
    if not source_path.exists() or not any(source_path.glob("*.parquet")):
        log.warning("No data found for '%s' at %s", source_name, source_path)
        return

    api = HfApi(token=token)
    api.create_repo(repo_id=repo_id, repo_type="dataset", exist_ok=True)

    log.info("Uploading %s → %s", source_path, repo_id)
    api.upload_folder(
        folder_path=str(source_path),
        repo_id=repo_id,
        repo_type="dataset",
    )
    log.info("Published '%s' to %s", source_name, repo_id)
