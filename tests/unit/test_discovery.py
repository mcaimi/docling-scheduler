#!/usr/bin/env python
"""S3 document discovery, mirroring the agent's listing rules."""

from docling_scheduler.discovery import list_s3_documents, s3_source_uri, shard_plan

from .fakes import FakeS3Client

KEYS = [
    "in/b.pdf",
    "in/a.md",
    "in/notes.txt",
    "in/UPPER.PDF",
    "in/nested/",
    "in/nested/c.docx",
    "inbox/d.pdf",
    "top.pdf",
]


def test_lists_direct_children_of_the_prefix_folder():
    client = FakeS3Client(KEYS)
    paths = list_s3_documents({"s3_bucket": "docs", "s3_prefix": "in"}, client)
    # prefix is a folder ("in/", not "inbox/"); globs are case-sensitive on the basename
    assert paths == ["s3://docs/in/a.md", "s3://docs/in/b.pdf"]
    assert client.calls == [{"Bucket": "docs", "Prefix": "in/", "Delimiter": "/"}]


def test_recursive_listing_skips_folder_placeholders():
    client = FakeS3Client(KEYS)
    paths = list_s3_documents({"s3_bucket": "docs", "s3_prefix": "in/", "s3_recursive": 1}, client)
    assert paths == ["s3://docs/in/a.md", "s3://docs/in/b.pdf", "s3://docs/in/nested/c.docx"]
    assert client.calls[0]["Delimiter"] is None


def test_empty_prefix_lists_the_bucket_root():
    paths = list_s3_documents({"s3_bucket": "docs", "s3_prefix": ""}, FakeS3Client(KEYS))
    assert paths == ["s3://docs/top.pdf"]
    assert s3_source_uri({"s3_bucket": "docs", "s3_prefix": ""}) == "s3://docs/"


def test_shard_plan_names_s3_objects():
    plan = shard_plan(["s3://docs/in/a.md", "s3://docs/in/b.pdf"], 2)
    assert [p["documents"] for p in plan] == [["a.md"], ["b.pdf"]]
