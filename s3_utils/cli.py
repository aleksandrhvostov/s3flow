from pathlib import Path
import typer

from .core import get_s3_client, list_prefixes, list_prefix_names
from .download import download_by_mask, download_file
from .copy import copy_by_mask, copy_multiple_prefixes, copy_common_and_addon_from_roots
from .move import move_by_mask
from .sync import sync_prefix
from .utils import read_yaml

app = typer.Typer(add_completion=False, help="S3 Toolkit CLI")

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


@app.command("download")
def cmd_download(
    prefix: str | None = typer.Option(None, help="S3 object prefix"),
    suffix: str | None = typer.Option(None, help="S3 object suffix filter"),
    bucket: str | None = typer.Option(None, help="S3 bucket"),
    out: Path | None = typer.Option(None, "--out", help="Local output directory"),
    config: str | None = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    """
    Download objects by prefix/suffix.
    """
    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)

    bucket = bucket or cfg["s3"]["bucket"]
    prefix = prefix if prefix is not None else cfg["examples"]["mask"]["prefix"]
    suffix = suffix if suffix is not None else cfg["examples"]["mask"]["suffix"]
    out_dir = Path(out or cfg["paths"]["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    keys = download_by_mask(s3, bucket, prefix=prefix, suffix=suffix, local_dir=str(out_dir))
    typer.echo(f"Downloaded {len(keys)} objects to {out_dir}")


@app.command("copy")
def cmd_copy(
    source_bucket: str | None = typer.Option(None, help="Source bucket"),
    target_bucket: str | None = typer.Option(None, help="Target bucket"),
    prefix: str | None = typer.Option(None, help="Source prefix"),
    suffix: str | None = typer.Option(None, help="Suffix filter"),
    prefix_dst: str | None = typer.Option(None, help="Destination prefix"),
    config: str | None = typer.Option(None, "--config", "-c"),
):
    """
    Copy objects by prefix/suffix.
    """
    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)

    ex = cfg["examples"]["copy"]
    source_bucket = source_bucket or ex["source_bucket"]
    target_bucket = target_bucket or ex["target_bucket"]
    prefix = prefix if prefix is not None else ex["prefix"]
    suffix = suffix if suffix is not None else ex["suffix"]
    prefix_dst = prefix_dst if prefix_dst is not None else ex["prefix_dst"]

    keys = copy_by_mask(s3, source_bucket, target_bucket, prefix=prefix, suffix=suffix, prefix_dst=prefix_dst)
    typer.echo(f"Copied {len(keys)} objects.")


@app.command("move")
def cmd_move(
    source_bucket: str | None = typer.Option(None),
    target_bucket: str | None = typer.Option(None),
    prefix: str | None = typer.Option(None),
    suffix: str | None = typer.Option(None),
    prefix_dst: str | None = typer.Option(None),
    config: str | None = typer.Option(None, "--config", "-c"),
):
    """
    Move objects by prefix/suffix.
    """
    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)
    ex = cfg["examples"]["move"]

    source_bucket = source_bucket or ex["source_bucket"]
    target_bucket = target_bucket or ex["target_bucket"]
    prefix = prefix if prefix is not None else ex["prefix"]
    suffix = suffix if suffix is not None else ex["suffix"]
    prefix_dst = prefix_dst if prefix_dst is not None else ex["prefix_dst"]

    keys = move_by_mask(s3, source_bucket, target_bucket, prefix=prefix, suffix=suffix, prefix_dst=prefix_dst)
    typer.echo(f"Moved {len(keys)} objects.")


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
    config: str | None = typer.Option(None, "--config", "-c", help="Path to YAML config"),
):
    """
    Sync objects between two prefixes.
    - compare-mode: name | etag | size
    - dry-run: show plan only
    - max-workers: parallelism for copy/delete
    - show-errors: print errors after sync
    """
    from .sync import sync_prefix  # local import to avoid circulars

    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)
    ex = cfg["examples"]["sync"]

    source_bucket = source_bucket or ex["source_bucket"]
    target_bucket = target_bucket or ex["target_bucket"]
    prefix_src = prefix_src if prefix_src is not None else ex["prefix_src"]
    prefix_dst = prefix_dst if prefix_dst is not None else ex["prefix_dst"]
    delete_extra = ex["delete_extra"] if delete_extra is None else delete_extra

    res = sync_prefix(
        s3_client=s3,
        source_bucket=source_bucket,
        target_bucket=target_bucket,
        prefix_src=prefix_src,
        prefix_dst=prefix_dst,
        delete_extra=delete_extra,
        compare_mode=compare_mode.lower(),
        max_workers=max_workers,
        dry_run=dry_run,
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
                typer.echo(f"  {e}")
            typer.echo("\n=== Delete Errors ===")
            for e in res["errors_delete"]:
                typer.echo(f"  {e}")
        else:
            typer.echo("\nNo errors to show")


@app.command("csv-download")
def cmd_csv_download(
    csv_file: Path | None = typer.Option(None, help="CSV with filenames"),
    column: str | None = typer.Option(None, help="Column that contains file names/paths"),
    s3_prefix: str | None = typer.Option(None, help="Prefix to prepend on S3"),
    bucket: str | None = typer.Option(None),
    out: Path | None = typer.Option(None, "--out"),
    max_workers: int | None = typer.Option(None),
    config: str | None = typer.Option(None, "--config", "-c"),
):
    """
    Download objects whose names are listed in a CSV column.
    """
    import pandas as pd
    import os

    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)

    bucket = bucket or cfg["s3"]["bucket"]
    out_dir = Path(out or cfg["paths"]["output_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    ex = cfg["examples"]["csv_download"]
    csv_file = Path(csv_file or ex["csv_file"])
    column = column or ex["column"]
    s3_prefix = s3_prefix if s3_prefix is not None else ex["s3_prefix"]
    max_workers = int(ex.get("max_workers", 8) if max_workers is None else max_workers)

    df = pd.read_csv(csv_file)
    values = df[column].dropna().unique().tolist()
    file_names = [Path(str(p)).name for p in values]

    from concurrent.futures import ThreadPoolExecutor
    def task(name: str):
        key = f"{s3_prefix}{name}"
        local_path = out_dir / name
        local_path.parent.mkdir(parents=True, exist_ok=True)
        download_file(s3, bucket, key, str(local_path))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for n in file_names:
            pool.submit(task, n)

    typer.echo(f"Downloaded {len(file_names)} files to {out_dir}")


@app.command("copy-common-addon")
def cmd_copy_common_addon(
    bucket: str | None = typer.Option(None),
    src_root: str | None = typer.Option(None, help="Source root prefix"),
    ref_root: str | None = typer.Option(None, help="Reference root prefix"),
    common_dst_root: str | None = typer.Option(None, help="Destination for common folders"),
    addon_dst_root: str | None = typer.Option(None, help="Destination for addon folders"),
    max_workers: int | None = typer.Option(None),
    config: str | None = typer.Option(None, "--config", "-c"),
):
    """
    Compare immediate child prefixes of two roots and copy:
    - common -> common_dst_root/<name>/
    - addon  -> addon_dst_root/<name>/
    """
    cfg = _load_cfg(config)
    s3 = _client_from_cfg(cfg)
    ex = cfg["examples"]["copy_common_addon"]

    bucket = bucket or ex["bucket"]
    src_root = src_root or ex["src_root"]
    ref_root = ref_root or ex["ref_root"]
    common_dst_root = common_dst_root or ex["common_dst_root"]
    addon_dst_root = addon_dst_root or ex["addon_dst_root"]
    max_workers = int(ex.get("max_workers", 8) if max_workers is None else max_workers)

    summary = copy_common_and_addon_from_roots(
        s3_client=s3,
        bucket=bucket,
        src_root_prefix=src_root,
        ref_root_prefix=ref_root,
        common_dst_root_prefix=common_dst_root,
        addon_dst_root_prefix=addon_dst_root,
        max_workers=max_workers,
    )
    typer.echo(f"Common: {summary['common_count']}, Addon: {summary['addon_count']}")
