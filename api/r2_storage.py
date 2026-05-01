import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class StorageObject:
    pathname: str
    uploaded_at: datetime
    size: int
    url: str


@dataclass(frozen=True)
class StoredObject:
    pathname: str
    url: str


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Set {name} before using Cloudflare R2 storage")
    return value


class R2StorageClient:
    def __init__(
        self,
        *,
        bucket_name: str | None = None,
        endpoint_url: str | None = None,
        public_base_url: str | None = None,
    ) -> None:
        account_id = os.environ.get("R2_ACCOUNT_ID")
        self.bucket_name = bucket_name or _required_env("R2_BUCKET_NAME")
        self.endpoint_url = endpoint_url or os.environ.get("R2_ENDPOINT_URL")
        if not self.endpoint_url:
            if not account_id:
                raise RuntimeError("Set R2_ENDPOINT_URL or R2_ACCOUNT_ID before using Cloudflare R2 storage")
            self.endpoint_url = f"https://{account_id}.r2.cloudflarestorage.com"

        self.public_base_url = (public_base_url or os.environ.get("R2_PUBLIC_BASE_URL") or "").rstrip("/")
        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=_required_env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=_required_env("R2_SECRET_ACCESS_KEY"),
            region_name=os.environ.get("R2_REGION", "auto"),
            config=Config(signature_version="s3v4"),
        )

    def object_url(self, pathname: str) -> str:
        if self.public_base_url:
            return f"{self.public_base_url}/{pathname.lstrip('/')}"
        return f"s3://{self.bucket_name}/{pathname}"

    def put(
        self,
        pathname: str,
        body: bytes,
        *,
        content_type: str,
        overwrite: bool = True,
    ) -> StoredObject:
        if not overwrite:
            try:
                self.client.head_object(Bucket=self.bucket_name, Key=pathname)
            except ClientError as exc:
                error_code = exc.response.get("Error", {}).get("Code")
                if error_code not in {"404", "NoSuchKey", "NotFound"}:
                    raise
            else:
                raise FileExistsError(pathname)

        self.client.put_object(
            Bucket=self.bucket_name,
            Key=pathname,
            Body=body,
            ContentType=content_type,
        )
        return StoredObject(pathname=pathname, url=self.object_url(pathname))

    def iter_objects(self, prefix: str, batch_size: int = 100):
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix,
            PaginationConfig={"PageSize": batch_size},
        )
        for page in pages:
            for item in page.get("Contents", []):
                pathname = item["Key"]
                yield StorageObject(
                    pathname=pathname,
                    uploaded_at=item["LastModified"],
                    size=item["Size"],
                    url=self.object_url(pathname),
                )

    def download_file(
        self,
        pathname: str,
        local_path: Path,
        *,
        overwrite: bool = True,
        create_parents: bool = True,
    ) -> None:
        if local_path.exists() and not overwrite:
            raise FileExistsError(local_path)
        if create_parents:
            local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket_name, pathname, str(local_path))

    def delete(self, pathname: str) -> None:
        self.client.delete_object(Bucket=self.bucket_name, Key=pathname)
