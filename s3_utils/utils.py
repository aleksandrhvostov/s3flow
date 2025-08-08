from __future__ import annotations
from typing import Iterable, Dict, List, Tuple
import re
import yaml

def filter_keys_by_mask(keys: Iterable[str], prefix: str = '', suffix: str = '') -> List[str]:
    """Filter object keys that start with *prefix* and end with *suffix*."""
    return [k for k in keys if k.startswith(prefix) and k.endswith(suffix)]

def is_s3_uri(uri: str) -> bool:
    return bool(re.match(r'^s3://[a-zA-Z0-9.\-_]+/.+', uri))

def parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not is_s3_uri(uri):
        raise ValueError(f"Invalid S3 URI: {uri}")
    parts = uri.replace("s3://", "").split("/", 1)
    return parts[0], parts[1]

def group_keys_by_prefix(keys: Iterable[str], depth: int = 1) -> Dict[str, List[str]]:
    """Group keys by first *depth* path components. Example: depth=2, key 'a/b/c.jpg' -> group 'a/b'."""
    groups: Dict[str, List[str]] = {}
    for key in keys:
        parts = key.split("/")
        group = "/".join(parts[:depth]) if parts else ""
        groups.setdefault(group, []).append(key)
    return groups

def read_yaml(path: str):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}
