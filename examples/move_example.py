from __future__ import annotations
from s3_utils.core import get_s3_client
from s3_utils.move import move_by_mask

if __name__ == "__main__":
    s3 = get_s3_client()
    res = move_by_mask(
        s3,
        source_bucket="my-source",
        target_bucket="my-target",
        prefix="tmp/",
        prefix_dst="archive/tmp/",
        progress=True,
        delete_batch_size=1000,
    )
    print("Moved:", len(res["moved"]), "Deleted source:", len(res["deleted_source"]))
    if res["errors_copy"] or res["errors_delete"]:
        print("Errors(copy/delete):", len(res["errors_copy"]), "/", len(res["errors_delete"]))
