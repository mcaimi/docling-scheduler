#!/usr/bin/env python
"""Run the scheduler API: ``python -m docling_scheduler [--port 8765]``."""

import ipaddress
import logging
import os
import sys
from argparse import ArgumentParser


def _is_loopback(host: str) -> bool:
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def main() -> int:
    parser = ArgumentParser(description="Docling agent scheduler API")
    parser.add_argument("--host", default="127.0.0.1", help="Loopback address to bind")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    if not _is_loopback(args.host):
        print("Refusing to bind a non-loopback address: the API can start containers.")
        return 2

    # an unreachable MLflow server must not stall API requests for minutes
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_MAX_RETRIES", "2")
    os.environ.setdefault("MLFLOW_HTTP_REQUEST_TIMEOUT", "10")

    import uvicorn

    from .api import create_app
    from .config import SchedulerConfig
    from .executors import create_executor
    from .mlflow_monitor import MlflowMonitor
    from .registry import Registry
    from .service import SchedulerService

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(message)s")
    config = SchedulerConfig.from_env()
    try:
        executor = create_executor(config)
    except ValueError as e:
        print(e)
        return 2
    service = SchedulerService(
        config,
        Registry(config.db_path),
        executor,
        MlflowMonitor(config.mlflow_tracking_uri, config.effective_mlflow_ui_uri),
    )
    host = "localhost" if args.host == "localhost" else args.host
    ui = config.web_dir is not None and (config.web_dir / "index.html").exists()
    print(f"Scheduler API on http://{host}:{args.port}  (executor: {executor.name})")
    if ui:
        # the token travels in the URL fragment, which browsers never send to servers
        print(f"  UI: http://{host}:{args.port}/#token={config.token}")
    else:
        print("  No UI: set SCHEDULER_WEB_DIR to a docling-ui build (dist/); API only")
    print(f"  token: {config.token}")
    print(f"  MLflow: {config.mlflow_tracking_uri}  registry: {config.db_path}")
    uvicorn.run(create_app(service, port=args.port), host=args.host, port=args.port, log_level="warning")
    return 0


if __name__ == "__main__":
    sys.exit(main())
