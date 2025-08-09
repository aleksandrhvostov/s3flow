from __future__ import annotations
from typing import Iterable, List, Tuple, Optional, Dict
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError
from tqdm import tqdm

from .core import list_objects
from .utils import relativize_keys


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def download_file(
    s3_client,
    bucket: str,
    key: str,
    dst_path: str | Path,
    overwrite: bool = False,
    extra_args: Optional[Dict] = None,
) -> Path:
    """
    Download a single S3 object to a local file.
    Returns the destination Path. Raises on error.
    """
    dst = Path(dst_path)
    if dst.exists() and not overwrite:
        return dst
    _ensure_parent(dst)
    if extra_args:
        # botocore doesn't take ExtraArgs for download_file; keep for API symmetry
        pass
    s3_client.download_file(bucket, key, str(dst))
    return dst


def _parallel_download(
    s3_client,
    bucket: str,
    pairs: Iterable[Tuple[str, Path]],
    max_workers: int = 8,
    progress: bool = False,
    overwrite: bool = False,
) -> Tuple[List[Tuple[str, Path]], List[str]]:
    """
    Download (key -> local_path) pairs in parallel.
    Returns: (downloaded_pairs, errors)
    """
    downloaded: List[Tuple[str, Path]] = []
    errors: List[str] = []

    pairs_list = list(pairs)
    bar = tqdm(total=len(pairs_list), desc="Download", unit="obj") if progress and pairs_list else None

    def _do(pair: Tuple[str, Path]) -> Tuple[str, Path]:
        k, dst = pair
        p = download_file(s3_client, bucket, k, dst, overwrite=overwrite)
        return (k, p)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(_do, p) for p in pairs_list]
        for f in as_completed(futs):
            try:
                item = f.result()
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
) -> Dict[str, List]:
    """
    Download all objects matching (prefix, suffix) to a local folder.
    If keep_structure=True, preserves S3 folder structure under dst_root.

    Returns dict:
      - downloaded: List[(s3_key, local_path)]
      - errors:     List[str]
      - stats:      Dict
    """
    keys = [k for k in list_objects(s3_client, bucket, prefix=prefix, suffix=suffix)]
    rel = relativize_keys(keys, prefix)

    dst_root = Path(dst_root)
    if keep_structure:
        pairs = []
        for r in rel:
            src_key = f"{prefix}{r}" if prefix else r
            local_path = dst_root / r
            pairs.append((src_key, local_path))
    else:
        # flatten into dst_root using the basename
        pairs = []
        for r in rel:
            src_key = f"{prefix}{r}" if prefix else r
            local_path = dst_root / Path(r).name
            pairs.append((src_key, local_path))

    downloaded, errors = _parallel_download(
        s3_client,
        bucket,
        pairs=pairs,
        max_workers=max_workers,
        progress=progress,
        overwrite=overwrite,
    )

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
            "total": len(pairs),
            "downloaded": len(downloaded),
            "errors_count": len(errors),
        },
    }
