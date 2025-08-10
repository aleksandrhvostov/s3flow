from __future__ import annotations
from typing import Callable, Iterable, Iterator, List, Tuple, Dict, Any, Optional
from pathlib import Path
import re
import fnmatch
import yaml
import os
from datetime import datetime, timezone


def ensure_dir(path: Path | str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def set_mtime(path: Path | str, dt: datetime) -> None:
    ts = int(dt.replace(tzinfo=timezone.utc).timestamp())
    os.utime(path, times=(ts, ts))


def read_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


_S3_URI_RE = re.compile(r"^s3://[a-zA-Z0-9.\-_/]+$")

def is_s3_uri(uri: str) -> bool:
    return bool(_S3_URI_RE.match(uri))


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not is_s3_uri(uri):
        raise ValueError(f"Invalid S3 URI: {uri}")
    bucket, key = uri.replace("s3://", "", 1).split("/", 1)
    return bucket, key


def relativize_keys(keys: Iterable[str], prefix: str | None) -> set[str]:
    rel: set[str] = set()
    p = prefix or ""
    for k in keys:
        if p and k.startswith(p):
            rel.add(k[len(p):])
        else:
            rel.add(k)
    return rel


def group_keys_by_prefix(keys: Iterable[str], depth: int = 1) -> Dict[str, List[str]]:
    groups: Dict[str, List[str]] = {}
    for key in keys:
        parts = key.split("/")
        group = "/".join(parts[:depth]) if parts else ""
        groups.setdefault(group, []).append(key)
    return groups


def compile_patterns(
    includes: Optional[list[str]] = None,
    excludes: Optional[list[str]] = None,
) -> Callable[[str], bool]:
    inc = list(includes or []) or ["*"]
    exc = list(excludes or [])

    def _match(name: str) -> bool:
        if any(fnmatch.fnmatch(name, pat) for pat in exc):
            return False
        return any(fnmatch.fnmatch(name, pat) for pat in inc)

    return _match


def filter_keys_by_mask(keys: Iterable[str], prefix: str = "", suffix: str = "") -> List[str]:
    return [k for k in keys if k.startswith(prefix) and k.endswith(suffix)]


def chunked(seq: Iterable[Any], size: int) -> Iterator[list[Any]]:
    if size <= 0:
        raise ValueError("size must be > 0")
    batch: list[Any] = []
    for item in seq:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    s = float(n)
    for u in units:
        if s < 1024 or u == units[-1]:
            return f"{s:.1f} {u}"
        s /= 1024.0


def get_s3_head(s3_client, bucket: str, key: str) -> Dict[str, Any]:
    try:
        h = s3_client.head_object(Bucket=bucket, Key=key)
    except Exception:
        return {}
    etag = (h.get("ETag") or "").strip('"')
    size = h.get("ContentLength")
    lm = h.get("LastModified")  # datetime with tz
    return {"ETag": etag, "ContentLength": size, "LastModified": lm}
