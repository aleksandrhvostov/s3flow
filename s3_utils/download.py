from __future__ import annotations
from typing import Iterable, List, Tuple, Optional, Dict, Literal
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from csv import DictWriter

from tqdm import tqdm

from .core import list_objects
from .utils import (
    relativize_keys,
    ensure_dir,
    get_s3_head,
    set_mtime,
    compile_patterns,
)

SkipMode = Literal["none", "size"]


def download_file(
    s3_client,
    bucket: str,
    key: str,
    dst_path: str | Path,
    overwrite: bool = False,
    preserve_mtime: bool = False,
) -> Path:
    dst = Path(dst_path)
    if dst.exists() and not overwrite:
        return dst
    ensure_dir(dst.parent)
    s3_client.download_file(bucket, key, str(dst))
    if preserve_mtime:
        meta = get_s3_head(s3_client, bucket, key)
        lm = meta.get("LastModified")
        if lm:
            set_mtime(dst, lm)
    return dst


def _parallel_download(
    s3_client,
    bucket: str,
    pairs: Iterable[Tuple[str, Path]],
    max_workers: int = 8,
    progress: bool = False,
    overwrite: bool = False,
    preserve_mtime: bool = False,
    skip_if: SkipMode = "none",
) -> Tuple[List[Tuple[str, Path]], List[str]]:
    downloaded: List[Tuple[str, Path]] = []
    errors: List[str] = []

    pairs_list = list(pairs)
    bar = tqdm(total=len(pairs_list), desc="Download", unit="obj") if progress and pairs_list else None

    def _do(pair: Tuple[str, Path]) -> Tuple[str, Path] | None:
        key, dst = pair
        if skip_if != "none" and dst.exists():
            if skip_if == "size":
                meta = get_s3_head(s3_client, bucket, key)
                size = meta.get("ContentLength")
                try:
                    local_size = dst.stat().st_size
                except Exception:
                    local_size = None
                if size is not None and local_size == size:
                    return None
        p = download_file(s3_client, bucket, key, dst, overwrite=overwrite, preserve_mtime=preserve_mtime)
        return (key, p)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_do, p) for p in pairs_list]
        for f in as_completed(futs):
            try:
                item = f.result()
                if item is not None:
                    downloaded.append(item)
            except Exception as e:
                errors.append(str(e))
            finally:
                if bar:
                    bar.update(1)

    if bar:
        bar.close()

    downloaded.sort(key=lambda x: x[0])
    return downloaded, errors


def download_by_mask(
    s3_client,
    bucket: str,
    prefix: str = "",
    suffix: str = "",
    dst_root: str | Path = ".",
    keep_structure: bool = True,
    overwrite: bool = False,
    max_workers: int = 8,
    progress: bool = False,
    dry_run: bool = False,
    skip_if: SkipMode = "none",
    preserve_mtime: bool = False,
    include: Optional[List[str]] = None,
    exclude: Optional[List[str]] = None,
    manifest_path: Optional[str | Path] = None,
) -> Dict[str, List]:
    keys = [k for k in list_objects(s3_client, bucket, prefix=prefix, suffix=suffix)]
    matcher = compile_patterns(includes=include, excludes=exclude) if (include or exclude) else (lambda _: True)
    keys = [k for k in keys if matcher(k)]

    rel = relativize_keys(keys, prefix)
    dst_root = Path(dst_root)

    pairs: List[Tuple[str, Path]] = []
    if keep_structure:
        for r in rel:
            src_key = f"{prefix}{r}" if prefix else r
            pairs.append((src_key, dst_root / r))
    else:
        for r in rel:
            src_key = f"{prefix}{r}" if prefix else r
            pairs.append((src_key, dst_root / Path(r).name))

    if dry_run:
        return {
            "downloaded": [],
            "errors": [],
            "stats": {
                "bucket": bucket,
                "prefix": prefix,
                "suffix": suffix,
                "dst_root": str(dst_root),
                "keep_structure": keep_structure,
                "overwrite": overwrite,
                "dry_run": True,
                "total": len(pairs),
                "planned": [(k, str(p)) for (k, p) in pairs],
            },
        }

    downloaded, errors = _parallel_download(
        s3_client,
        bucket,
        pairs=pairs,
        max_workers=max_workers,
        progress=progress,
        overwrite=overwrite,
        preserve_mtime=preserve_mtime,
        skip_if=skip_if,
    )

    if manifest_path:
        ensure_dir(Path(manifest_path).parent)
        with open(manifest_path, "w", newline="", encoding="utf-8") as f:
            w = DictWriter(f, fieldnames=["key", "local_path"])
            w.writeheader()
            for k, p in downloaded:
                w.writerow({"key": k, "local_path": str(p)})

    return {
        "downloaded": [(k, str(p)) for (k, p) in downloaded],
        "errors": errors,
        "stats": {
            "bucket": bucket,
            "prefix": prefix,
            "suffix": suffix,
            "dst_root": str(dst_root),
            "keep_structure": keep_structure,
            "overwrite": overwrite,
            "dry_run": False,
            "skip_if": skip_if,
            "preserve_mtime": preserve_mtime,
            "total": len(pairs),
            "downloaded": len(downloaded),
            "errors_count": len(errors),
        },
    }
