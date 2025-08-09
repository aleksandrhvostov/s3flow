from pathlib import Path
import logging
import typer

from .core import get_s3_client, list_prefixes, list_prefix_names
from .download import download_by_mask, download_file
from .copy import copy_by_mask, copy_multiple_prefixes, copy_common_and_addon_from_roots
from .move import move_by_mask
from .sync import sync_prefix
from .utils import read_yaml
from .errors import setup_logging

app = typer.Typer(add_completion=False, help="S3 Toolkit CLI")

@app.callback()
def _root(verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging")):
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(level=level)

DEFAULT_CONFIG = "config/config.yaml"

def _load_cfg(config_path: str | None) -> dict:
    path = config_path or DEFAULT_CONFIG
    cfg = read_yaml(path)
    if not cfg:
        raise typer.BadParameter(f"Config is empty or not found: {path}")
    return cfg

def _client_from_cfg(cfg: dict):
    aws = cfg.get("aws", {})
    return get_s3_client(aws_profile=aws.get("profile"), region_name=aws.get("region"))

# ----------------
# SYNC
# ----------------
@app.command("sync")
def cmd_sync(
    source_bucket: str | None = typer.Option(None, help="Source bucket"),
    target_bucket: str | None = typer.Option(None, help="Target bucket"),
    prefix_src: str | None = typer.Option(None, help="Source prefix"),
    prefix_dst: str | None = typer.Option(None, help="Destination prefix"),
    delete_extra: bool | None = typer.Option(None, help="Delete extra objects on target"),
    compare_mode: str = typer.Option(
        "name",
        help="Comparison mode",
        case_sensitive=False,
        click_type=typer.Choice(["name", "etag", "size"], case_sensitive=False),
    ),
    dry_run: bool = typer.Option(False, help="Plan only; do not modify anything"),
    max_workers: int = typer.Option(8, help="Parallel workers for copy/delete"),
    show_errors: bool = typer.Option(False, help="Print error details after sync"),
    progress: bool = typer.Option(False, help="Show progress bar"),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)
    ex = cfg["examples"]["sync"]

    source_bucket = source_bucket or ex["source_bucket"]
    target_bucket = target_bucket or ex["target_bucket"]
    prefix_src = prefix_src if prefix_src is not None else ex["prefix_src"]
    prefix_dst = prefix_dst if prefix_dst is not None else ex["prefix_dst"]
    delete_extra = ex["delete_extra"] if delete_extra is None else delete_extra

    res = sync_prefix(
        s3,
        source_bucket,
        target_bucket,
        prefix_src=prefix_src or "",
        prefix_dst=prefix_dst or "",
        delete_extra=bool(delete_extra),
        compare_mode=compare_mode,
        max_workers=max_workers,
        dry_run=dry_run,
        progress=progress,
    )

    typer.echo(
        f"Synced. Copied: {len(res['copied'])}, "
        f"Deleted: {len(res['deleted'])}, "
        f"Errors(copy/delete): {len(res['errors_copy'])}/{len(res['errors_delete'])}, "
        f"Mode: {res['stats']['compare_mode']}, Dry-run: {res['stats']['dry_run']}"
    )
    if show_errors and (res["errors_copy"] or res["errors_delete"]):
        typer.echo("\n=== Copy Errors ===")
        for e in res["errors_copy"]:
            typer.echo(e)
        typer.echo("\n=== Delete Errors ===")
        for e in res["errors_delete"]:
            typer.echo(e)

# ----------------
# DOWNLOAD
# ----------------
@app.command("download")
def cmd_download(
    bucket: str | None = typer.Option(None, help="Source bucket"),
    prefix: str | None = typer.Option(None, help="Key prefix"),
    suffix: str | None = typer.Option("", help="Key suffix filter"),
    dst_root: str | None = typer.Option(".", help="Local destination directory"),
    keep_structure: bool = typer.Option(True, help="Preserve S3 folder structure"),
    overwrite: bool = typer.Option(False, help="Overwrite existing files"),
    max_workers: int = typer.Option(8, help="Parallel workers"),
    progress: bool = typer.Option(False, help="Show progress bar"),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)
    ex = cfg["examples"]["download"]

    bucket = bucket or ex["bucket"]
    prefix = prefix if prefix is not None else ex["prefix"]
    suffix = suffix if suffix is not None else ex.get("suffix", "")
    dst_root = dst_root or ex.get("dst_root", ".")
    keep_structure = bool(keep_structure if keep_structure is not None else ex.get("keep_structure", True))
    overwrite = bool(overwrite if overwrite is not None else ex.get("overwrite", False))

    res = download_by_mask(
        s3,
        bucket=bucket,
        prefix=prefix or "",
        suffix=suffix or "",
        dst_root=dst_root,
        keep_structure=keep_structure,
        overwrite=overwrite,
        max_workers=max_workers,
        progress=progress,
    )

    typer.echo(
        f"Downloaded: {len(res['downloaded'])}, "
        f"Errors: {len(res['errors'])}, "
        f"Dst: {res['stats']['dst_root']}, Keep-structure: {res['stats']['keep_structure']}, "
        f"Overwrite: {res['stats']['overwrite']}"
    )
    if res["errors"]:
        typer.echo("\n=== Download Errors ===")
        for e in res["errors"]:
            typer.echo(e)

# ----------------
# MOVE
# ----------------
@app.command("move")
def cmd_move(
    source_bucket: str | None = typer.Option(None, help="Source bucket"),
    target_bucket: str | None = typer.Option(None, help="Target bucket"),
    prefix: str | None = typer.Option(None, help="Source prefix"),
    suffix: str | None = typer.Option("", help="Key suffix filter"),
    prefix_dst: str | None = typer.Option(None, help="Destination prefix"),
    max_workers: int = typer.Option(8, help="Parallel workers"),
    progress: bool = typer.Option(False, help="Show progress bar"),
    dry_run: bool = typer.Option(False, help="Plan only; do not modify anything"),
    delete_batch_size: int = typer.Option(1000, help="Batch size for delete (<=1000)"),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)
    ex = cfg["examples"]["move"]

    source_bucket = source_bucket or ex["source_bucket"]
    target_bucket = target_bucket or ex["target_bucket"]
    prefix = prefix if prefix is not None else ex["prefix"]
    suffix = suffix if suffix is not None else ex.get("suffix", "")
    prefix_dst = prefix_dst if prefix_dst is not None else ex["prefix_dst"]

    res = move_by_mask(
        s3,
        source_bucket=source_bucket,
        target_bucket=target_bucket,
        prefix=prefix or "",
        suffix=suffix or "",
        prefix_dst=prefix_dst or "",
        max_workers=max_workers,
        progress=progress,
        dry_run=dry_run,
        delete_batch_size=delete_batch_size,
    )

    typer.echo(
        f"Moved: {len(res['moved'])}, "
        f"Deleted source: {len(res['deleted_source'])}, "
        f"Errors(copy/delete): {len(res['errors_copy'])}/{len(res['errors_delete'])}, "
        f"Dry-run: {res['stats']['dry_run']}"
    )
    if res["errors_copy"] or res["errors_delete"]:
        typer.echo("\n=== Copy Errors ===")
        for e in res["errors_copy"]:
            typer.echo(e)
        typer.echo("\n=== Delete Errors ===")
        for e in res["errors_delete"]:
            typer.echo(e)
