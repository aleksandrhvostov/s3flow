from __future__ import annotations
from s3_utils.core import get_s3_client
from s3_utils.sync import sync_prefix

if __name__ == "__main__":
    s3 = get_s3_client()
    res = sync_prefix(
        s3,
        source_bucket="my-source",
        target_bucket="my-target",
        prefix_src="data/v1/",
        prefix_dst="data/v1/",
        delete_extra=True,
        progress=True,
    )
    print("Copied:", len(res["copied"]), "Deleted:", len(res["deleted"]))
    if res["errors_copy"] or res["errors_delete"]:
        print("Errors(copy/delete):", len(res["errors_copy"]), "/", len(res["errors_delete"]))
