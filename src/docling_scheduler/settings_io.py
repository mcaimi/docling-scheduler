#!/usr/bin/env python
"""Settings -> container environment rendering for scheduler-launched runs.

Form values are trusted operator input; they are validated against
the agent's settings contract (``contract.py``) and rendered into plain environment variables (non-secret)
and runtime secrets (``sensitive`` settings). ``user_prompt`` is passed through
unchanged: NLP extraction still runs inside the container, under the
extractor's own rules for untrusted input.
"""

import json
import os
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

from dotenv import dotenv_values

from .contract import SETTINGS_BY_KEY, SETTINGS_SPEC, SettingSpec, settings_from_mapping

CONTAINER_DATA_DIR = "/data/in"
# Where a host ``s3_ca_bundle`` file is mounted read-only.
CONTAINER_S3_CA_BUNDLE = "/data/s3-ca-bundle.pem"
CONTAINER_HOST_GATEWAY = "host.containers.internal"

# Set by the scheduler for every container; requests may not override them.
MANAGED_KEYS = frozenset(
    {
        "folder_path",
        "shard_index",
        "shard_count",
        "enable_tracing",
        "log_format",
        "mlflow_tracking_uri",
        "mlflow_parent_run_id",
        "mlflow_session_id",
        "scheduler_run_id",
        "scheduler_executor",
    }
)

# Settings holding a URL, or a bare host, that a container must be able to reach.
URL_KEYS = ("base_url", "embedding_base_url", "vlm_api_url", "milvus_uri", "s3_endpoint_url")
HOST_KEYS = ("chroma_host",)
_LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1", "0.0.0.0"}

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def load_defaults(envfile: str | None) -> dict[str, Any]:
    """Resolve defaults like the CLI: ``.env`` values, overridden by the
    scheduler's own environment, then spec defaults. ``os.environ`` is never
    modified.
    """
    file_values = {k: v for k, v in dotenv_values(envfile).items() if v is not None} if envfile else {}
    return settings_from_mapping({**file_values, **os.environ})


def coerce_value(spec: SettingSpec, value: Any) -> Any:
    """Validate/convert one form value to the type the pipeline expects.

    Raises ``ValueError`` with a user-facing message.
    """
    if value is None or (isinstance(value, str) and not value.strip()):
        return spec.resolve_default()
    if spec.type == "int":
        if isinstance(value, bool):
            raise ValueError("expected an integer")
        try:
            return int(str(value).strip())
        except ValueError:
            raise ValueError("expected an integer") from None
    if spec.type == "bool":
        if isinstance(value, bool):
            return int(value)
        lowered = str(value).strip().lower()
        if lowered in _TRUE:
            return 1
        if lowered in _FALSE:
            return 0
        raise ValueError("expected a boolean")
    if spec.type == "json":
        parsed = value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as e:
                raise ValueError(f"invalid JSON: {e}") from None
        if not isinstance(parsed, dict):
            raise ValueError("expected a JSON object")
        return parsed
    if not isinstance(value, (str, int, float)):
        raise ValueError("expected a string")
    text = str(value).strip()
    if spec.choices and text not in spec.choices:
        raise ValueError(f"must be one of {', '.join(spec.choices)}")
    return text


def merge_settings(
    defaults: Mapping[str, Any], overrides: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, str], set[str]]:
    """Apply form overrides to defaults.

    Returns ``(settings, errors, explicit_keys)`` where ``errors`` maps a key to
    a message and ``explicit_keys`` are the keys the request set. Sensitive
    keys left empty in the request keep their default (from ``.env``).
    """
    settings = dict(defaults)
    errors: dict[str, str] = {}
    explicit: set[str] = set()
    for key, value in overrides.items():
        spec = SETTINGS_BY_KEY.get(key)
        if spec is None:
            errors[key] = "unknown setting"
            continue
        if key in MANAGED_KEYS:
            errors[key] = "set by the scheduler; cannot be overridden"
            continue
        if spec.sensitive and (value is None or value == ""):
            continue
        try:
            settings[key] = coerce_value(spec, value)
        except ValueError as e:
            errors[key] = str(e)
            continue
        explicit.add(key)
    return settings, errors, explicit


def is_loopback_host(host: str | None) -> bool:
    return bool(host) and host.lower().strip("[]") in _LOOPBACK_HOSTS


def is_loopback_url(url: str) -> bool:
    try:
        return is_loopback_host(urlsplit(url).hostname)
    except ValueError:
        return False


def rewrite_loopback_url(url: str, gateway: str = CONTAINER_HOST_GATEWAY) -> str | None:
    """Return ``url`` with a loopback host replaced by ``gateway``, or
    ``None`` if it doesn't point at loopback."""
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    if not is_loopback_host(parts.hostname):
        return None
    netloc = gateway
    if parts.port:
        netloc = f"{netloc}:{parts.port}"
    if parts.username:
        auth = parts.username + (f":{parts.password}" if parts.password else "")
        netloc = f"{auth}@{netloc}"
    return urlunsplit(parts._replace(netloc=netloc))


def setting_in_use(key: str, settings: Mapping[str, Any]) -> bool:
    """Whether the contract's ``depends_on`` conditions hold (``milvus_uri``
    only matters with ``vector_store_backend=milvus``)."""
    return all(
        settings.get(other) in values
        for other, values in SETTINGS_BY_KEY[key].depends_on.items()
    )


def loopback_keys(settings: Mapping[str, Any], in_use_only: bool = False) -> list[str]:
    """Endpoint settings (URLs and bare hosts) that point at loopback; with
    ``in_use_only``, only those the agent will use."""
    keys = [
        key for key in URL_KEYS
        if isinstance(settings.get(key), str) and is_loopback_url(settings[key])
    ]
    keys += [
        key for key in HOST_KEYS
        if isinstance(settings.get(key), str) and is_loopback_host(settings[key])
    ]
    return [key for key in keys if not in_use_only or setting_in_use(key, settings)]


def rewrite_loopback(
    settings: dict[str, Any], gateway: str = CONTAINER_HOST_GATEWAY
) -> tuple[dict[str, Any], list[str]]:
    """Point loopback endpoints at the container host gateway.

    Inside a container ``localhost`` is the container itself, so services the
    operator runs on the host must be reached via the gateway
    (``host.containers.internal`` for Podman). Returns the rewritten settings
    and the keys that changed.
    """
    out = dict(settings)
    changed = loopback_keys(settings)
    for key in changed:
        out[key] = rewrite_loopback_url(out[key], gateway) if key in URL_KEYS else gateway
    return out, changed


def _env_string(spec: SettingSpec, value: Any) -> str:
    if spec.type == "json":
        return json.dumps(value)
    return str(value)


def render_environment(
    settings: Mapping[str, Any],
    *,
    explicit_keys: set[str],
    managed: Mapping[str, str],
) -> tuple[dict[str, str], dict[str, str]]:
    """Render settings to ``(env, secret_env)`` for a container.

    - Managed keys come only from ``managed`` (scheduler-controlled values).
    - ``None`` values are omitted (the container falls back to spec defaults).
    - Settings with a callable default (``max_concurrency`` = CPU count) are
      only rendered when set explicitly: the scheduler host's CPU count says
      nothing about the container's.
    - Settings that depend on ``source_backend`` (``s3_*``) are only rendered
      for that backend: ``.env``/``AWS_*`` credentials don't reach local runs.
      Safe because the agent never takes ``source_backend`` from ``user_prompt``.
    - ``sensitive`` settings go to ``secret_env``, never to ``env``.
    """
    env: dict[str, str] = {}
    secret_env: dict[str, str] = {}
    for spec in SETTINGS_SPEC:
        if spec.key in MANAGED_KEYS:
            continue
        value = settings.get(spec.key)
        if value is None:
            continue
        if callable(spec.default) and spec.key not in explicit_keys:
            continue
        backends = spec.depends_on.get("source_backend")
        if backends and settings.get("source_backend") not in backends:
            continue
        target = secret_env if spec.sensitive else env
        target[spec.env] = _env_string(spec, value)
    env.update(managed)
    return env, secret_env


def non_secret_settings(settings: Mapping[str, Any]) -> dict[str, Any]:
    """Settings safe to persist and show: sensitive values are dropped."""
    return {k: v for k, v in settings.items() if not SETTINGS_BY_KEY[k].sensitive}


def secret_presence(settings: Mapping[str, Any]) -> dict[str, bool]:
    """Sensitive keys -> whether a value is set (never the value itself)."""
    return {
        spec.key: bool(settings.get(spec.key))
        for spec in SETTINGS_SPEC
        if spec.sensitive
    }


__all__ = [
    "CONTAINER_DATA_DIR",
    "CONTAINER_S3_CA_BUNDLE",
    "CONTAINER_HOST_GATEWAY",
    "MANAGED_KEYS",
    "load_defaults",
    "coerce_value",
    "merge_settings",
    "is_loopback_host",
    "is_loopback_url",
    "loopback_keys",
    "setting_in_use",
    "rewrite_loopback",
    "rewrite_loopback_url",
    "render_environment",
    "non_secret_settings",
    "secret_presence",
]
