#!/usr/bin/env python
"""Settings validation and container env rendering."""

import pytest

from docling_scheduler.discovery import DOCUMENT_GLOBS, list_documents, shard_plan
from docling_scheduler.settings_io import (
    CONTAINER_HOST_GATEWAY,
    MANAGED_KEYS,
    coerce_value,
    load_defaults,
    merge_settings,
    render_environment,
    rewrite_loopback,
    rewrite_loopback_url,
    secret_presence,
)
from docling_scheduler.contract import SETTINGS_BY_KEY, SETTINGS_SPEC


@pytest.fixture
def defaults(monkeypatch):
    for spec in SETTINGS_SPEC:
        for key in (spec.env, *spec.env_fallbacks):
            monkeypatch.delenv(key, raising=False)
    return load_defaults(None)


def test_discovery_globs_come_from_contract():
    assert DOCUMENT_GLOBS == ("*.pdf", "*.docx", "*.xlsx", "*.md", "*.html", "*.latex", "*.tex")


def test_shard_plan_matches_pipeline_split(tmp_path):
    for name in ("a.md", "b.pdf", "c.docx", "d.html", "skip.txt"):
        (tmp_path / name).write_text("x")
    plan = shard_plan(list_documents(tmp_path), 3)
    assert [p["documents"] for p in plan] == [["a.md", "d.html"], ["b.pdf"], ["c.docx"]]


def test_load_defaults_reads_envfile_without_touching_environ(tmp_path, monkeypatch, defaults):
    import os

    envfile = tmp_path / ".env"
    envfile.write_text("CHUNK_SIZE=777\nEMBEDDING_API_KEY=sk-file\n")
    monkeypatch.setenv("CHUNK_OVERLAP", "11")  # process env wins over the file
    resolved = load_defaults(str(envfile))
    assert resolved["chunk_size"] == 777
    assert resolved["chunk_overlap"] == 11
    assert resolved["embedding_api_key"] == "sk-file"
    assert "CHUNK_SIZE" not in os.environ


@pytest.mark.parametrize(
    "key, value, expected",
    [
        ("chunk_size", "500", 500),
        ("chunk_size", 500, 500),
        ("chunk_size", "", 1000),
        ("chroma_allow_reset", True, 1),
        ("chroma_allow_reset", "off", 0),
        ("vlm_api_headers", '{"a": "b"}', {"a": "b"}),
        ("vlm_api_headers", {"a": "b"}, {"a": "b"}),
        ("chunking_strategy", " hybrid ", "hybrid"),
        ("chroma_host", "", None),
    ],
)
def test_coerce_value(key, value, expected):
    assert coerce_value(SETTINGS_BY_KEY[key], value) == expected


@pytest.mark.parametrize(
    "key, value",
    [
        ("chunk_size", "big"),
        ("chunk_size", True),
        ("chroma_allow_reset", "maybe"),
        ("vlm_api_headers", "[1]"),
        ("vlm_api_headers", "{bad json"),
        ("chunking_strategy", "semantic"),
        ("llm_model", ["x"]),
    ],
)
def test_coerce_value_rejects(key, value):
    with pytest.raises(ValueError):
        coerce_value(SETTINGS_BY_KEY[key], value)


def test_merge_settings(defaults):
    settings, errors, explicit = merge_settings(
        {**defaults, "embedding_api_key": "sk-env"},
        {
            "chunk_size": "200",
            "embedding_api_key": "",  # empty secret keeps the .env value
            "folder_path": "/etc",  # managed
            "nope": 1,
            "chunk_overlap": "x",
        },
    )
    assert settings["chunk_size"] == 200
    assert settings["embedding_api_key"] == "sk-env"
    assert explicit == {"chunk_size"}
    assert set(errors) == {"folder_path", "nope", "chunk_overlap"}


def test_managed_keys_exist_in_spec():
    assert MANAGED_KEYS <= set(SETTINGS_BY_KEY)


@pytest.mark.parametrize(
    "url, expected",
    [
        ("http://localhost:5000", f"http://{CONTAINER_HOST_GATEWAY}:5000"),
        ("http://127.0.0.1/v1", f"http://{CONTAINER_HOST_GATEWAY}/v1"),
        ("https://user:pw@localhost:8443/x?y=1", f"https://user:pw@{CONTAINER_HOST_GATEWAY}:8443/x?y=1"),
        ("http://[::1]:19530", f"http://{CONTAINER_HOST_GATEWAY}:19530"),
        ("https://api.openai.com/v1", None),
    ],
)
def test_rewrite_loopback_url(url, expected):
    assert rewrite_loopback_url(url) == expected


def test_rewrite_loopback_settings(defaults):
    settings, changed = rewrite_loopback(
        {**defaults, "milvus_uri": "http://localhost:19530", "chroma_host": "127.0.0.1"}
    )
    assert settings["milvus_uri"] == f"http://{CONTAINER_HOST_GATEWAY}:19530"
    assert settings["chroma_host"] == CONTAINER_HOST_GATEWAY
    assert set(changed) == {"milvus_uri", "chroma_host"}


def test_render_environment_splits_secrets_and_skips_managed(defaults):
    settings = {
        **defaults,
        "embedding_api_key": "sk-1",
        "vlm_api_headers": {"Authorization": "Bearer x"},
        "folder_path": "/should/not/leak",
        "chunk_size": 300,
    }
    env, secret_env = render_environment(
        settings, explicit_keys={"chunk_size"}, managed={"FOLDER_PATH": "/data/in"}
    )
    assert secret_env["EMBEDDING_API_KEY"] == "sk-1"
    assert secret_env["VLM_API_HEADERS"] == '{"Authorization": "Bearer x"}'
    assert "EMBEDDING_API_KEY" not in env and "VLM_API_HEADERS" not in env
    assert env["FOLDER_PATH"] == "/data/in"
    assert env["CHUNK_SIZE"] == "300"
    # callable default (host CPU count) only rendered when explicit
    assert "MAX_CONCURRENCY" not in env
    # None values omitted
    assert "CHROMA_HOST" not in env
    for spec in SETTINGS_SPEC:
        if spec.key in MANAGED_KEYS and spec.env != "FOLDER_PATH":
            assert spec.env not in env


def test_secret_presence_never_returns_values(defaults):
    presence = secret_presence({**defaults, "api_key": "sk-x", "milvus_token": None})
    assert presence["api_key"] is True
    assert presence["milvus_token"] is False
    assert "sk-x" not in str(presence)


def test_s3_settings_only_rendered_for_s3_source(defaults, monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIA-host")
    monkeypatch.setenv("S3_BUCKET", "docs")
    settings = load_defaults(None)
    assert settings["s3_access_key_id"] == "AKIA-host"  # env fallback

    env, secret_env = render_environment(settings, explicit_keys=set(), managed={})
    assert not any(name.startswith("S3_") for name in (*env, *secret_env))

    env, secret_env = render_environment(
        {**settings, "source_backend": "s3"}, explicit_keys=set(), managed={}
    )
    assert env["SOURCE_BACKEND"] == "s3"
    assert env["S3_BUCKET"] == "docs"
    assert secret_env["S3_ACCESS_KEY_ID"] == "AKIA-host"
    assert "S3_ACCESS_KEY_ID" not in env


def test_s3_endpoint_loopback_rewritten(defaults):
    settings, changed = rewrite_loopback({**defaults, "s3_endpoint_url": "http://localhost:9000"})
    assert settings["s3_endpoint_url"] == f"http://{CONTAINER_HOST_GATEWAY}:9000"
    assert "s3_endpoint_url" in changed
