#!/usr/bin/env python
"""The vendored agent settings contract and env resolution."""

import json

import pytest

from docling_scheduler.contract import (
    SETTINGS_BY_KEY,
    SUPPORTED_SCHEMA_VERSION,
    load_contract,
    settings_from_mapping,
)


def _contract(**overrides):
    entry = {"key": "chunk_size", "env": "CHUNK_SIZE", "type": "int", "default": 1000,
             "dynamic_default": None, "group": "chunking"}
    return json.dumps({"schema_version": SUPPORTED_SCHEMA_VERSION, "document_globs": ["*.md"],
                       "settings": [{**entry, **overrides}]})


def test_vendored_contract_loads():
    assert SETTINGS_BY_KEY["embedding_api_key"].sensitive
    assert SETTINGS_BY_KEY["embedding_api_key"].default is None
    assert SETTINGS_BY_KEY["shard_index"].env_fallbacks == ("JOB_COMPLETION_INDEX",)
    assert callable(SETTINGS_BY_KEY["max_concurrency"].default)


def test_unsupported_version_rejected():
    with pytest.raises(ValueError, match="schema_version"):
        load_contract(json.dumps({"schema_version": 99, "document_globs": [], "settings": []}))


def test_unknown_dynamic_default_rejected():
    with pytest.raises(ValueError, match="dynamic_default"):
        load_contract(_contract(default=None, dynamic_default="gpu_count"))


def test_env_resolution_matches_agent_rules():
    resolved = settings_from_mapping({
        "CHUNK_SIZE": " 250 ",
        "CHUNK_OVERLAP": "not-a-number",  # unparseable -> default
        "ENABLE_TRACING": "yes",
        "JOB_COMPLETION_INDEX": "3",  # fallback env var
        "CHROMA_COLLECTION_METADATA": '{"a": 1}',
        "LOG_LEVEL": "   ",  # blank -> default
    })
    assert resolved["chunk_size"] == 250
    assert resolved["chunk_overlap"] == 100
    assert resolved["enable_tracing"] == 1
    assert resolved["shard_index"] == 3
    assert resolved["chroma_collection_metadata"] == {"a": 1}
    assert resolved["log_level"] == "INFO"
    with pytest.raises(ValueError, match="JSON object"):
        settings_from_mapping({"VLM_API_HEADERS": "[1]"})
