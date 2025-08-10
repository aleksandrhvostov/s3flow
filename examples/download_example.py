from __future__ import annotations
from s3_utils.core import get_s3_client
from s3_utils.download import download_by_mask

if __name__ == "__main__":
    s3 = get_s3_client()
    res = download_by_mask(
        s3,
        bucket="my-bucket",
        prefix="images/",
        suffix=".jpg",
        dst_root="downloads",
        keep_structure=True,
        overwrite=False,
        progress=True,
        max_workers=8,
    )
    print("Downloaded:", len(res["downloaded"]), "Errors:", len(res["errors"]))
