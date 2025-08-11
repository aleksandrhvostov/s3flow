# cli.py
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, List

import typer
import click

from .core import get_s3_client
from .download import download_by_mask
from .move import move_by_mask
from .sync import sync_prefix
from .utils import read_yaml, parse_s3_uri
from .errors import setup_logging

app = typer.Typer(add_completion=False, help="S3 Toolkit CLI")

# ---------------- Settings kept in Typer context ----------------
@dataclass
class Settings:
    verbose: bool = False
    aws_profile: Optional[str] = None
    aws_region: Optional[str] = None

DEFAULT_CONFIG = "config/config.yaml"

# ---------------- Helpers ----------------
def _load_cfg(config_path: Optional[str]) -> dict:
    """
    Load YAML config if present, otherwise return {}.
    Never crash on missing/empty config.
    """
    path = config_path or DEFAULT_CONFIG
    try:
        cfg = read_yaml(path)
    except FileNotFoundError:
        return {}
    if not cfg:
        return {}
    return cfg

def _client_from_cfg(cfg: dict, settings: Settings):
    """
    Resolve AWS auth/region with priority:
    CLI flags -> ENV (handled inside boto3) -> YAML.
    """
    aws = (cfg.get("aws") or {}) if cfg else {}
    return get_s3_client(
        aws_profile=settings.aws_profile or aws.get("profile"),
        aws_access_key_id=aws.get("access_key_id"),
        aws_secret_access_key=aws.get("secret_access_key"),
        region_name=settings.aws_region or aws.get("region"),
        retries_max_attempts=aws.get("retries_max_attempts", 8),
        retries_mode=aws.get("retries_mode", "standard"),
        connect_timeout=aws.get("connect_timeout", 10),
        read_timeout=aws.get("read_timeout", 60),
    )

def _parse_patterns(csv: Optional[str]) -> Optional[List[str]]:
    if csv is None:
        return None
    csv = csv.strip()
    if not csv:
        return []
    return [p.strip() for p in csv.split(",") if p.strip()]

# ---------------- Root options (global) ----------------
@app.callback()
def _root(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose logging"),
    profile: Optional[str] = typer.Option(None, "--profile", help="AWS profile name"),
    region: Optional[str] = typer.Option(None, "--region", help="AWS region (e.g. us-east-1)"),
):
    """
    Set up global Settings and logging once.
    """
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(level=level)

    ctx.obj = Settings(
        verbose=verbose,
        aws_profile=profile,
        aws_region=region,
    )

# ---------------- SYNC ----------------
@app.command("sync")
def cmd_sync(
    ctx: typer.Context,
    source: Optional[str] = typer.Option(None, "--src", help="Source S3 URI (e.g. s3://bucket/prefix/)"),
    target: Optional[str] = typer.Option(None, "--dst", help="Destination S3 URI"),
    delete_extra: bool = typer.Option(False, help="Delete extra keys on target"),
    compare_mode: str = typer.Option(
        "name",
        help="Comparison mode",
        case_sensitive=False,
        click_type=click.Choice(["name", "etag", "size"], case_sensitive=False),
    ),
    dry_run: bool = typer.Option(False, help="Plan only; do not modify anything"),
    max_workers: int = typer.Option(8, help="Parallel workers"),
    progress: bool = typer.Option(False, help="Show progress bar"),
    show_errors: bool = typer.Option(False, "--show-errors/--no-show-errors", help="Print failed keys"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg, ctx.obj)
    scfg = (cfg.get("sync") or {}) if cfg else {}

    src_uri = source or scfg.get("src")
    dst_uri = target or scfg.get("dst")
    if not src_uri or not dst_uri:
        raise typer.BadParameter("Provide --src and --dst or set sync.src and sync.dst in config.yaml")

    src_bucket, src_prefix = parse_s3_uri(src_uri)
    dst_bucket, dst_prefix = parse_s3_uri(dst_uri)

    cm = (compare_mode or scfg.get("compare_mode", "name")).lower()

    res = sync_prefix(
        s3,
        src_bucket,
        dst_bucket,
        prefix_src=src_prefix,
        prefix_dst=dst_prefix,
        delete_extra=scfg.get("delete_extra", delete_extra),
        compare_mode=cm,
        max_workers=scfg.get("max_workers", max_workers),
        dry_run=scfg.get("dry_run", dry_run),
        progress=scfg.get("progress", progress),
    )

    typer.echo(
        f"Synced. Copied: {len(res['copied'])}, Deleted: {len(res['deleted'])}, "
        f"Errors(copy/delete): {len(res['errors_copy'])}/{len(res['errors_delete'])}, "
        f"Mode: {res['stats']['compare_mode']}, Dry-run: {res['stats']['dry_run']}"
    )

    if show_errors:
        for e in res.get("errors_copy", []):
            typer.echo(f"[COPY ERROR] {e}")
        for e in res.get("errors_delete", []):
            typer.echo(f"[DELETE ERROR] {e}")

# ---------------- DOWNLOAD ----------------
@app.command("download")
def cmd_download(
    ctx: typer.Context,
    source: Optional[str] = typer.Option(None, "--from", help="Source S3 URI (e.g. s3://bucket/prefix/)"),
    to: str = typer.Option("./downloads", "--to", help="Local destination directory"),
    suffix: Optional[str] = typer.Option(None, help="Suffix filter (e.g. .jpg)"),
    keep_structure: bool = typer.Option(False, "--keep-structure/--no-keep-structure", help="Preserve S3 folder structure"),
    overwrite: bool = typer.Option(False, "--overwrite/--no-overwrite", help="Overwrite existing files"),
    max_workers: int = typer.Option(8, help="Parallel workers"),
    progress: bool = typer.Option(False, "--progress/--no-progress", help="Show progress bar"),
    dry_run: bool = typer.Option(False, "--dry-run/--no-dry-run", help="Plan only; do not download files"),
    skip_if: str = typer.Option(
        "none",
        help="Skip downloads if",
        case_sensitive=False,
        click_type=click.Choice(["none", "size"], case_sensitive=False),
    ),
    preserve_mtime: bool = typer.Option(False, "--preserve-mtime/--no-preserve-mtime", help="Set local mtime to S3 LastModified"),
    include: Optional[str] = typer.Option(None, help="Comma-separated glob patterns to include"),
    exclude: Optional[str] = typer.Option(None, help="Comma-separated glob patterns to exclude"),
    manifest: Optional[str] = typer.Option(None, "--manifest", help="Write CSV manifest of downloaded files"),
    show_errors: bool = typer.Option(False, "--show-errors/--no-show-errors", help="Print failed keys"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    log = logging.getLogger("s3_utils.cli.download")
    cfg = _load_cfg(config)
    dcfg = (cfg.get("download") or {}) if cfg else {}

    from_uri = source or dcfg.get("from")
    if not from_uri:
        raise typer.BadParameter("Provide --from or set download.from in config.yaml")

    bucket, prefix = parse_s3_uri(from_uri)

    # resolve values: CLI flag -> YAML
    dst = to or dcfg.get("to", "./downloads")
    suffix_val = suffix if suffix is not None else dcfg.get("suffix", "")
    keep_val = dcfg.get("keep_structure", keep_structure)
    overwrite_val = dcfg.get("overwrite", overwrite)
    dry_run_val = dcfg.get("dry_run", dry_run)
    progress_val = dcfg.get("progress", progress)
    skip_val = (skip_if or dcfg.get("skip_if", "none")).lower()
    preserve_mtime_val = dcfg.get("preserve_mtime", preserve_mtime)
    include_val = _parse_patterns(include if include is not None else dcfg.get("include", None))
    exclude_val = _parse_patterns(exclude if exclude is not None else dcfg.get("exclude", None))
    manifest_val = manifest or dcfg.get("manifest", None)

    s3 = _client_from_cfg(cfg, ctx.obj)

    res = download_by_mask(
        s3,
        bucket=bucket,
        prefix=prefix,
        suffix=suffix_val,
        dst_root=dst,
        keep_structure=keep_val,
        overwrite=overwrite_val,
        max_workers=dcfg.get("max_workers", max_workers),
        progress=progress_val,
        dry_run=dry_run_val,
        skip_if=skip_val,                # "none" | "size"
        preserve_mtime=preserve_mtime_val,
        include=include_val,
        exclude=exclude_val,
        manifest_path=manifest_val,
    )

    if dry_run_val:
        log.info("Planned: %d items (dry-run), Dest=%s", res["stats"]["total"], res["stats"]["dst_root"])
        if show_errors and res.get("errors"):
            for e in res["errors"]:
                typer.echo(f"[PLAN ERROR] {e}")
        return

    log.info(
        "Downloaded=%d Errors=%d Dest=%s Keep-structure=%s Overwrite=%s Skip-if=%s Preserve-mtime=%s",
        len(res["downloaded"]),
        len(res["errors"]),
        res["stats"]["dst_root"],
        res["stats"]["keep_structure"],
        res["stats"]["overwrite"],
        res["stats"]["skip_if"],
        res["stats"]["preserve_mtime"],
    )

    if show_errors and res.get("errors"):
        for e in res["errors"]:
            typer.echo(f"[ERROR] {e}")

    if res.get("errors"):
        raise typer.Exit(code=1)

# ---------------- MOVE ----------------
@app.command("move")
def cmd_move(
    ctx: typer.Context,
    source: Optional[str] = typer.Option(None, "--src", help="Source S3 URI"),
    target: Optional[str] = typer.Option(None, "--dst", help="Destination S3 URI"),
    suffix: Optional[str] = typer.Option(None, help="Suffix filter (optional)"),
    dry_run: bool = typer.Option(False, "--dry-run/--no-dry-run", help="Plan only; do not modify anything"),
    max_workers: int = typer.Option(8, help="Parallel workers"),
    progress: bool = typer.Option(False, "--progress/--no-progress", help="Show progress bar"),
    delete_batch_size: int = typer.Option(1000, help="Batch size for delete (<=1000)"),
    show_errors: bool = typer.Option(False, "--show-errors/--no-show-errors", help="Print failed keys"),
    config: Optional[str] = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    cfg = _load_cfg(config)
    mcfg = (cfg.get("move") or {}) if cfg else {}

    src_uri = source or mcfg.get("src")
    dst_uri = target or mcfg.get("dst")
    if not src_uri or not dst_uri:
        raise typer.BadParameter("Provide --src and --dst or set move.src and move.dst in config.yaml")

    sb, sp = parse_s3_uri(src_uri)
    tb, tp = parse_s3_uri(dst_uri)

    s3 = _client_from_cfg(cfg, ctx.obj)

    res = move_by_mask(
        s3,
        source_bucket=sb,
        target_bucket=tb,
        prefix=sp,
        suffix=(suffix if suffix is not None else mcfg.get("suffix", "")),
        prefix_dst=tp,
        max_workers=mcfg.get("max_workers", max_workers),
        progress=mcfg.get("progress", progress),
        dry_run=mcfg.get("dry_run", dry_run),
        delete_batch_size=mcfg.get("delete_batch_size", delete_batch_size),
    )

    typer.echo(
        f"Moved: {len(res['moved'])}, Deleted source: {len(res['deleted_source'])}, "
        f"Errors(copy/delete): {len(res['errors_copy'])}/{len(res['errors_delete'])}, "
        f"Dry-run: {res['stats']['dry_run']}"
    )

    if show_errors:
        for e in res.get("errors_copy", []):
            typer.echo(f"[COPY ERROR] {e}")
        for e in res.get("errors_delete", []):
            typer.echo(f"[DELETE ERROR] {e}")
