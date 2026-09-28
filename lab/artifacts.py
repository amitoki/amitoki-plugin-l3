"""コンテナで生成した観測ファイルを、試験を起動した利用者へ戻す。"""
import os


def restore_ownership(directory):
    if "L3_OWNER_UID" not in os.environ or "L3_OWNER_GID" not in os.environ:
        return
    owner = int(os.environ["L3_OWNER_UID"]), int(os.environ["L3_OWNER_GID"])
    for path in [directory, *directory.rglob("*")]:
        os.chown(path, *owner, follow_symlinks=False)
