#!/usr/bin/env python
"""Document discovery for pre-flight, as the agent containers will see it.

``DOCUMENT_GLOBS`` comes from the agent's settings contract; the shard split
mirrors the agent's ``shard_paths`` (sorted paths, ``paths[i::N]``). S3
listing mirrors the agent's ``list_s3_document_paths``: the prefix is a
folder, globs match the key's basename (case-sensitive), ``/``-terminated
placeholder keys are skipped, and only ``s3_recursive`` descends into nested
prefixes.
"""

import fnmatch
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from .contract import DOCUMENT_GLOBS

S3_SCHEME = "s3://"


def list_documents(folder: str | Path) -> list[str]:
    root = Path(folder)
    paths: list[str] = []
    for pattern in DOCUMENT_GLOBS:
        paths.extend(str(p) for p in root.glob(pattern))
    paths.sort()
    return paths


def s3_prefix(settings: Mapping[str, Any]) -> str:
    prefix = settings.get("s3_prefix") or ""
    return prefix if not prefix or prefix.endswith("/") else prefix + "/"


def s3_source_uri(settings: Mapping[str, Any]) -> str:
    return f"{S3_SCHEME}{settings.get('s3_bucket')}/{s3_prefix(settings)}"


def s3_client(settings: Mapping[str, Any]) -> Any:
    """An S3 client for the connection settings, as seen from this host.

    Unset credentials fall through to boto3's default chain, as in the agent.
    Short timeouts: this runs inside a pre-flight request.
    """
    import boto3
    from botocore.config import Config

    verify: bool | str = bool(settings.get("s3_verify_ssl", 1))
    if verify and settings.get("s3_ca_bundle"):
        verify = settings["s3_ca_bundle"]
    return boto3.session.Session().client(
        "s3",
        endpoint_url=settings.get("s3_endpoint_url") or None,
        region_name=settings.get("s3_region") or None,
        aws_access_key_id=settings.get("s3_access_key_id") or None,
        aws_secret_access_key=settings.get("s3_secret_access_key") or None,
        aws_session_token=settings.get("s3_session_token") or None,
        verify=verify,
        config=Config(
            connect_timeout=5,
            read_timeout=10,
            retries={"mode": "standard", "max_attempts": 2},
            s3={"addressing_style": settings.get("s3_addressing_style") or "auto"},
        ),
    )


def list_s3_documents(settings: Mapping[str, Any], client: Any = None) -> list[str]:
    """Sorted ``s3://bucket/key`` URIs of the documents the agent will list."""
    bucket = settings.get("s3_bucket")
    params: dict[str, Any] = {"Bucket": bucket, "Prefix": s3_prefix(settings)}
    if not settings.get("s3_recursive"):
        params["Delimiter"] = "/"
    client = client or s3_client(settings)
    paths: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(**params):
        for obj in page.get("Contents", ()):
            key = obj["Key"]
            name = PurePosixPath(key).name
            if key.endswith("/") or not any(fnmatch.fnmatchcase(name, p) for p in DOCUMENT_GLOBS):
                continue
            paths.append(f"{S3_SCHEME}{bucket}/{key}")
    paths.sort()
    return paths


def shard_plan(paths: list[str], shard_count: int) -> list[dict]:
    """Per-shard document counts and names, as the containers will split them."""
    return [
        {
            "shard_index": i,
            "document_count": len(paths[i::shard_count]),
            "documents": [PurePosixPath(p).name for p in paths[i::shard_count]],
        }
        for i in range(shard_count)
    ]


__all__ = [
    "DOCUMENT_GLOBS",
    "list_documents",
    "list_s3_documents",
    "s3_client",
    "s3_prefix",
    "s3_source_uri",
    "shard_plan",
]
