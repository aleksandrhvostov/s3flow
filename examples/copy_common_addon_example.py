from s3_utils.core import get_s3_client
from s3_utils.copy import copy_common_and_addon_from_roots
from s3_utils.utils import read_yaml

CONFIG_PATH = "config/config.yaml"

if __name__ == "__main__":
    cfg = read_yaml(CONFIG_PATH)

    aws = cfg["aws"]
    ex  = cfg["examples"]["copy_common_addon"]

    s3 = get_s3_client(
        aws_profile=aws.get("profile"),
        region_name=aws.get("region")
    )

    summary = copy_common_and_addon_from_roots(
        s3_client=s3,
        bucket=ex["bucket"],
        src_root_prefix=ex["src_root"],
        ref_root_prefix=ex["ref_root"],
        common_dst_root_prefix=ex["common_dst_root"],
        addon_dst_root_prefix=ex["addon_dst_root"],
        max_workers=int(ex.get("max_workers", 8)),
    )

    print(f"Common folders: {summary['common_count']}")
    print(f"Addon folders:  {summary['addon_count']}")
    # Optional: print per-folder stats
    # print("Common detail:", summary["common"])
    # print("Addon  detail:", summary["addon"])
