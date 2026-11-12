#!/usr/bin/env python
"""The agent's settings contract, loaded from the vendored ``settings-schema.json``.

The scheduler never imports agent code. The agent publishes its settings
(env var, type, default, ``sensitive``, ...) and the document globs it
processes as JSON; refresh the vendored copy with::

    python -m src.docling_agent --print-settings-schema \\
        > src/docling_scheduler/settings-schema.json

Env values resolve exactly as the agent's loader does: the env var, then its
fallbacks, otherwise the default; unparseable int/bool values fall back to the
default and invalid JSON raises.
"""

import json
import os
from dataclasses import dataclass, field
from importlib import resources
from typing import Any, Callable, Literal, Mapping

SUPPORTED_SCHEMA_VERSION = 1

SettingType = Literal["str", "int", "bool", "json"]

# Contract names of computed defaults, resolved on this host.
_DYNAMIC_DEFAULTS: dict[str, Callable[[], Any]] = {"cpu_count": os.cpu_count}

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


@dataclass(frozen=True)
class SettingSpec:
    key: str
    env: str
    type: SettingType
    # A callable for computed defaults (``dynamic_default`` in the contract).
    # Sensitive settings have no default in the contract: ``None``.
    default: Any | Callable[[], Any]
    group: str
    description: str = ""
    choices: tuple[str, ...] | None = None
    sensitive: bool = False
    untrusted: bool = False
    env_fallbacks: tuple[str, ...] = ()
    depends_on: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def resolve_default(self) -> Any:
        return self.default() if callable(self.default) else self.default


def _spec_from_entry(entry: dict[str, Any]) -> SettingSpec:
    dynamic = entry.get("dynamic_default")
    if dynamic is not None and dynamic not in _DYNAMIC_DEFAULTS:
        raise ValueError(f"{entry['key']}: unknown dynamic_default {dynamic!r}")
    return SettingSpec(
        key=entry["key"],
        env=entry["env"],
        type=entry["type"],
        default=_DYNAMIC_DEFAULTS[dynamic] if dynamic else entry.get("default"),
        group=entry["group"],
        description=entry.get("description", ""),
        choices=tuple(entry["choices"]) if entry.get("choices") else None,
        sensitive=bool(entry.get("sensitive")),
        untrusted=bool(entry.get("untrusted")),
        env_fallbacks=tuple(entry.get("env_fallbacks") or ()),
        depends_on={k: tuple(v) for k, v in (entry.get("depends_on") or {}).items()},
    )


def load_contract(text: str) -> tuple[tuple[SettingSpec, ...], tuple[str, ...]]:
    """Parse a contract document into ``(settings, document_globs)``."""
    data = json.loads(text)
    version = data.get("schema_version")
    if version != SUPPORTED_SCHEMA_VERSION:
        raise ValueError(
            f"settings contract schema_version {version!r} is not supported "
            f"(expected {SUPPORTED_SCHEMA_VERSION})"
        )
    specs = tuple(_spec_from_entry(entry) for entry in data["settings"])
    return specs, tuple(data["document_globs"])


SETTINGS_SPEC, DOCUMENT_GLOBS = load_contract(
    resources.files(__package__).joinpath("settings-schema.json").read_text(encoding="utf-8")
)
SETTINGS_BY_KEY: dict[str, SettingSpec] = {s.key: s for s in SETTINGS_SPEC}


def _raw_value(spec: SettingSpec, environ: Mapping[str, str]) -> str | None:
    for key in (spec.env, *spec.env_fallbacks):
        raw = environ.get(key)
        if raw is not None and raw.strip():
            return raw.strip()
    return None


def _parse_json_object(raw: str) -> dict[str, Any]:
    try:
        out = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}") from e
    if not isinstance(out, dict):
        raise ValueError(f"JSON must be a JSON object, got {type(out).__name__}")
    return out


def _resolve_setting(spec: SettingSpec, environ: Mapping[str, str]) -> Any:
    raw = _raw_value(spec, environ)
    if raw is None:
        return spec.resolve_default()
    if spec.type == "int":
        try:
            return int(raw)
        except ValueError:
            return spec.resolve_default()
    if spec.type == "bool":
        lowered = raw.lower()
        if lowered in _TRUE:
            return 1
        if lowered in _FALSE:
            return 0
        try:
            return int(int(raw) != 0)
        except ValueError:
            return spec.resolve_default()
    if spec.type == "json":
        return _parse_json_object(raw)
    return raw


def settings_from_mapping(environ: Mapping[str, str]) -> dict[str, Any]:
    """Resolve every setting from ``environ`` (env var names -> values)."""
    return {spec.key: _resolve_setting(spec, environ) for spec in SETTINGS_SPEC}


def settings_schema() -> list[dict[str, Any]]:
    """JSON-serialisable view of the spec for the UI form.

    Computed defaults are resolved on this host (a display hint only: they
    are never rendered into a container unless set explicitly).
    """
    return [
        {
            "key": s.key,
            "env": s.env,
            "type": s.type,
            "default": None if s.sensitive else s.resolve_default(),
            "group": s.group,
            "description": s.description,
            "choices": list(s.choices) if s.choices else None,
            "sensitive": s.sensitive,
            "untrusted": s.untrusted,
            "env_fallbacks": list(s.env_fallbacks),
            "depends_on": {k: list(v) for k, v in s.depends_on.items()},
        }
        for s in SETTINGS_SPEC
    ]


__all__ = [
    "SettingSpec",
    "SETTINGS_SPEC",
    "SETTINGS_BY_KEY",
    "DOCUMENT_GLOBS",
    "SUPPORTED_SCHEMA_VERSION",
    "load_contract",
    "settings_from_mapping",
    "settings_schema",
]
