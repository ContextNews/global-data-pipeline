"""Typer CLI — entry point: `gdp`."""

from __future__ import annotations

import typer

from global_data_pipeline.config import settings
from global_data_pipeline.logging import get_logger, setup_logging
from global_data_pipeline.sources import ALL_SOURCES
from global_data_pipeline.storage.state import PipelineState
import global_data_pipeline.storage.local as local_store

log = get_logger("cli")

app = typer.Typer(
    help="Global Data Pipeline — extract macroeconomic timeseries from WB, IMF, and UN.",
    no_args_is_help=True,
)

_SOURCE_HELP = "Source name (world_bank | imf | un_sdg) or 'all'."


def _resolve_sources(source: str) -> list[str]:
    if source == "all":
        return list(ALL_SOURCES.keys())
    if source not in ALL_SOURCES:
        typer.echo(f"Unknown source '{source}'. Available: {', '.join(ALL_SOURCES)}", err=True)
        raise typer.Exit(1)
    return [source]


@app.command()
def discover(
    source: str = typer.Argument(..., help=_SOURCE_HELP),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """List all discoverable indicators for a source (dry run, no data written)."""
    setup_logging(verbose)
    for name in _resolve_sources(source):
        src = ALL_SOURCES[name]()
        indicators = src.discover()
        typer.echo(f"{name}: {len(indicators)} indicators")
        if verbose:
            for ind in indicators:
                typer.echo(f"  {ind.code}  {ind.name}")


@app.command()
def extract(
    source: str = typer.Argument(..., help=_SOURCE_HELP),
    full: bool = typer.Option(False, "--full", help="Force full rebuild, ignoring saved state."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Extract, transform, and save data locally."""
    setup_logging(verbose)
    settings.ensure_dirs()
    for name in _resolve_sources(source):
        _run_extract(name, full=full)


@app.command()
def status() -> None:
    """Show last-run times, indicator counts, and local data sizes."""
    setup_logging()
    for name in ALL_SOURCES:
        state = PipelineState(settings.state_dir, name)
        stats = local_store.source_stats(settings.datasets_dir, name)
        typer.echo(
            f"{name}: last_run={state.last_run or 'never'}, "
            f"indicators={state.indicator_count()}, "
            f"files={stats['files']}, rows={stats['total_rows']}, "
            f"size={stats['size_mb']} MB"
        )


@app.command()
def publish(
    source: str = typer.Argument(..., help=_SOURCE_HELP),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Publish local Parquet data to Hugging Face."""
    setup_logging(verbose)
    if not settings.hf_token:
        typer.echo("HF_TOKEN not set in .env — cannot publish.", err=True)
        raise typer.Exit(1)
    from global_data_pipeline.publish.huggingface import publish_source

    for name in _resolve_sources(source):
        publish_source(name, settings.datasets_dir, token=settings.hf_token)


@app.command()
def load(
    source: str = typer.Argument(..., help=_SOURCE_HELP),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Load local Parquet data into Neon PostgreSQL (Phase 3)."""
    setup_logging(verbose)
    if not settings.database_url:
        typer.echo("DATABASE_URL not set in .env — cannot load.", err=True)
        raise typer.Exit(1)
    from global_data_pipeline.publish.database import load_source

    for name in _resolve_sources(source):
        load_source(name, settings.datasets_dir, settings.database_url)


@app.command()
def run(
    source: str = typer.Option("all", "--source", help=_SOURCE_HELP),
    no_publish: bool = typer.Option(False, "--no-publish", help="Skip Hugging Face publishing."),
    full: bool = typer.Option(False, "--full", help="Force full rebuild."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run the full pipeline: extract then publish."""
    setup_logging(verbose)
    settings.ensure_dirs()
    names = _resolve_sources(source)
    for name in names:
        _run_extract(name, full=full)
    if not no_publish:
        if not settings.hf_token:
            typer.echo("HF_TOKEN not set — skipping publish.", err=True)
            return
        from global_data_pipeline.publish.huggingface import publish_source

        for name in names:
            publish_source(name, settings.datasets_dir, token=settings.hf_token)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _run_extract(source_name: str, *, full: bool) -> None:
    src = ALL_SOURCES[source_name]()
    state = PipelineState(settings.state_dir, source_name)

    typer.echo(f"[{source_name}] Discovering indicators...")
    indicators = src.discover()
    typer.echo(f"[{source_name}] {len(indicators)} indicators found")

    skipped = extracted = failed = 0

    for indicator in indicators:
        prev = state.get_indicator(indicator.code)
        if not full and prev is not None:
            if state.should_skip(indicator.code, prev.get("last_updated")):
                skipped += 1
                continue

        df = src.extract_and_transform(indicator)
        if df.empty:
            log.debug("No data for %s — skipping write", indicator.code)
            failed += 1
            continue

        path = local_store.write_indicator(settings.datasets_dir, source_name, indicator.code, df)
        checksum = PipelineState.compute_checksum(path.read_bytes())
        state.update_indicator(indicator.code, row_count=len(df), checksum=checksum)
        state.save()
        extracted += 1

    typer.echo(
        f"[{source_name}] Done — extracted: {extracted}, skipped: {skipped}, failed: {failed}"
    )


