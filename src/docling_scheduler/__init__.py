#!/usr/bin/env python
"""Scheduler for containerised docling-agent runs.

The scheduler never imports agent code: it renders settings (described by the
agent's vendored settings contract) into container environment
variables/secrets, launches the agent image under Podman, and monitors
progress through MLflow and the container runtime.
"""
