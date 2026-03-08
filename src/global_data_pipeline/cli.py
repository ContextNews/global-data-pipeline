"""Typer CLI — entry point: `gdp`."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

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
    workers: int = typer.Option(0, "--workers", "-w", help="Parallel workers (0 = per-source default)."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Extract, transform, and save data locally."""
    setup_logging(verbose)
    settings.ensure_dirs()
    for name in _resolve_sources(source):
        _run_extract(name, full=full, workers=workers or None)


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
    workers: int = typer.Option(0, "--workers", "-w", help="Parallel workers (0 = per-source default)."),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Run the full pipeline: extract then publish."""
    setup_logging(verbose)
    settings.ensure_dirs()
    names = _resolve_sources(source)
    for name in names:
        _run_extract(name, full=full, workers=workers or None)
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

# Default parallel workers per source.
# IMF: kept at 1 — imfp manages its own rate limiting (10 req/5 s).
_DEFAULT_WORKERS: dict[str, int] = {"world_bank": 8, "imf": 1, "un_sdg": 4}


def _run_extract(source_name: str, *, full: bool, workers: int | None = None) -> None:
    src = ALL_SOURCES[source_name]()
    state = PipelineState(settings.state_dir, source_name)
    n_workers = workers if workers is not None else _DEFAULT_WORKERS.get(source_name, 4)

    typer.echo(f"[{source_name}] Discovering indicators...")
    indicators = src.discover()
    typer.echo(f"[{source_name}] {len(indicators)} indicators found")

    # Skip anything already in state — supports resuming interrupted runs.
    # Use --full to re-extract everything.
    to_extract = [ind for ind in indicators if full or state.get_indicator(ind.code) is None]
    skipped = len(indicators) - len(to_extract)
    if skipped:
        typer.echo(f"[{source_name}] Resuming — skipping {skipped} already-extracted indicators")

    total = len(to_extract)
    extracted = failed = 0

    def _fetch(indicator):
        df = src.extract_and_transform(indicator)
        if df.empty:
            return indicator.code, None
        local_store.write_indicator(settings.datasets_dir, source_name, indicator.code, df)
        return indicator.code, len(df)

    with ThreadPoolExecutor(max_workers=n_workers) as executor:
        futures = {executor.submit(_fetch, ind): ind for ind in to_extract}
        for completed, future in enumerate(as_completed(futures), 1):
            code, row_count = future.result()
            if row_count is None:
                log.warning("[%d/%d] %s — no data", completed, total, code)
                failed += 1
            else:
                log.info("[%d/%d] %s — %d rows", completed, total, code, row_count)
                state.update_indicator(code, row_count=row_count)
                extracted += 1

            # Flush state to disk every 10 completions to bound checkpoint cost
            if completed % 10 == 0:
                state.save()

    state.save()
    typer.echo(
        f"[{source_name}] Done — extracted: {extracted}, skipped: {skipped}, failed: {failed}"
    )


