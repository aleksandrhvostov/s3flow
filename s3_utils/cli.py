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

@app.command("sync")
def cmd_sync(
    source_bucket: str | None = typer.Option(None, help="Source bucket"),
    target_bucket: str | None = typer.Option(None, help="Target bucket"),
    prefix_src: str | None = typer.Option(None, help="Source prefix"),
    prefix_dst: str | None = typer.Option(None, help="Destination prefix"),
    delete_extra: bool | None = typer.Option(None, help="Delete extra objects on target"),
    compare_mode: str = typer.Option(
        "name",
        help="How to compare existing objects to decide copying",
        case_sensitive=False,
        click_type=typer.Choice(["name", "etag", "size"], case_sensitive=False),
    ),
    dry_run: bool = typer.Option(False, help="Plan only; do not modify anything"),
    max_workers: int = typer.Option(8, help="Parallel workers for copy/delete"),
    show_errors: bool = typer.Option(False, help="Print error details after sync"),
    progress: bool = typer.Option(False, help="Show progress bar for copy/delete"),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    from .sync import sync_prefix

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

    if show_errors:
        if res["errors_copy"] or res["errors_delete"]:
            typer.echo("\n=== Copy Errors ===")
            for e in res["errors_copy"]:
                typer.echo(e)
            typer.echo("\n=== Delete Errors ===")
            for e in res["errors_delete"]:
                typer.echo(e)
