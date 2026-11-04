"""Object-storage side of scripts/backup-db.sh and scripts/restore-db.sh.

Any S3-compatible store works: AWS S3, Hetzner Object Storage, Cloudflare R2,
Backblaze B2, MinIO. Configured from the environment:

  BACKUP_S3_BUCKET             bucket name (required)
  BACKUP_S3_ENDPOINT           endpoint URL; blank for AWS
  BACKUP_S3_REGION             region; blank means us-east-1 (R2 takes "auto")
  BACKUP_S3_ACCESS_KEY_ID      credentials; blank falls back to boto3's own
  BACKUP_S3_SECRET_ACCESS_KEY  lookup (instance roles, ~/.aws)
  BACKUP_S3_PREFIX             key prefix, default "barrelman/"

Usage:
  backup-upload.py upload <file>        prints the key it wrote
  backup-upload.py prune <days>         deletes backups older than <days>
  backup-upload.py latest               prints the newest backup's key
  backup-upload.py download <key> <dest>
  backup-upload.py list

Only keys named like a backup (<prefix>barrelman-*.dump) are ever listed or
pruned, so a bucket shared with other data is safe.
"""

import os
import re
import sys
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

BACKUP_NAME = re.compile(r"^barrelman-[0-9TZ]+\.dump$")


def env(name, default=""):
    value = os.environ.get(name, "").strip()
    return value or default


BUCKET = env("BACKUP_S3_BUCKET")
PREFIX = env("BACKUP_S3_PREFIX", "barrelman/")


def client():
    # boto3 1.36+ adds CRC checksums to every upload by default. Several
    # S3-compatible stores reject or mishandle them, so only send them where
    # the API requires one.
    config = Config(
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
        retries={"max_attempts": 5, "mode": "standard"},
    )
    return boto3.client(
        "s3",
        endpoint_url=env("BACKUP_S3_ENDPOINT") or None,
        region_name=env("BACKUP_S3_REGION", "us-east-1"),
        aws_access_key_id=env("BACKUP_S3_ACCESS_KEY_ID") or None,
        aws_secret_access_key=env("BACKUP_S3_SECRET_ACCESS_KEY") or None,
        config=config,
    )


def backups(s3):
    """Every backup under the prefix, oldest first."""
    found = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            if BACKUP_NAME.match(obj["Key"][len(PREFIX):]):
                found.append(obj)
    return sorted(found, key=lambda o: o["Key"])


def upload(path):
    key = PREFIX + os.path.basename(path)
    s3 = client()
    s3.upload_file(path, BUCKET, key)
    # Read it back: a 200 from a misconfigured proxy is not a stored object.
    size = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
    if size != os.path.getsize(path):
        sys.exit(f"uploaded {key} is {size} bytes, expected {os.path.getsize(path)}")
    print(key)


def prune(days):
    s3 = client()
    found = backups(s3)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    # The newest backup always survives, whatever its age: a box whose
    # backups have been failing for a month should not also lose the last
    # good one off-site.
    stale = [o for o in found[:-1] if o["LastModified"] < cutoff]
    for obj in stale:
        s3.delete_object(Bucket=BUCKET, Key=obj["Key"])
        print(f"deleted {obj['Key']}")
    print(f"{len(found) - len(stale)} remote backups kept, {len(stale)} deleted")


def latest():
    found = backups(client())
    if not found:
        sys.exit(f"no backups under s3://{BUCKET}/{PREFIX}")
    print(found[-1]["Key"])


def download(key, dest):
    client().download_file(BUCKET, key, dest)


def list_backups():
    for obj in backups(client()):
        print(f"{obj['Key']}\t{obj['Size']}\t{obj['LastModified'].isoformat()}")


def main(argv):
    if not BUCKET:
        sys.exit("BACKUP_S3_BUCKET is not set")
    cmd, args = (argv[0], argv[1:]) if argv else ("", [])
    if cmd == "upload" and len(args) == 1:
        upload(args[0])
    elif cmd == "prune" and len(args) == 1 and args[0].isdigit():
        prune(int(args[0]))
    elif cmd == "latest" and not args:
        latest()
    elif cmd == "download" and len(args) == 2:
        download(*args)
    elif cmd == "list" and not args:
        list_backups()
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
