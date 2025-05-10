#!/usr/bin/env python3
"""shared/logger_utils.py – common structured‑logging helpers for the demo APIs

Example
~~~~~~~
```python
from shared.logger_utils import setup_metrics_logger, log_rpc

log = setup_metrics_logger(                   # all args optional
    name="api.metrics",                     # logger name
    log_file_path="/var/log/my/metrics.jsonl",  # default: logs/api_metrics.jsonl
    max_mb=25,                               # rotate when file >25 MiB (default 10)
    backup_cnt=14,                           # keep 14 backups (default 5)
)
```
"""
from __future__ import annotations

import json
import logging
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

__all__ = [
    "setup_metrics_logger",
    "log_rpc",
]

# ─────────────────────────────────────────────────────────────────────────────
# Logger factory
# ─────────────────────────────────────────────────────────────────────────────


def setup_metrics_logger(
    name: str = "api.metrics",
    *,
    log_file_path: Optional[str] = None,
    max_mb: int = 10,
    backup_cnt: int = 5,
) -> logging.Logger:  # noqa: D401
    """Return a *configured* logger that writes JSON lines to stdout + file.

    Parameters
    ----------
    name:
        Logger name (hierarchical path). Defaults to ``api.metrics``.
    log_file_path:
        Destination path on disk. Defaults to ``logs/<name>.jsonl`` relative to
        the current working directory.
    max_mb:
        Rotate the file when it exceeds *max_mb* MiB (default: 10 MiB).
    backup_cnt:
        Number of historical log files to keep (default: 5).
    """

    # Root logger emits to stdout exactly once (idempotent call).
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=False,
    )

    log = logging.getLogger(name)
    log.setLevel(logging.INFO)

    # Resolve file path (either explicit or default convention)
    if log_file_path is None:
        log_file_path = f"logs/{name.replace('.', '_')}.jsonl"
    path = Path(log_file_path).expanduser()

    # Attach rotating file handler only once per process
    if not any(isinstance(h, RotatingFileHandler) for h in log.handlers):
        path.parent.mkdir(parents=True, exist_ok=True)

        file_hdlr = RotatingFileHandler(
            path,
            maxBytes=max_mb * 1024 * 1024,
            backupCount=backup_cnt,
        )
        file_hdlr.setFormatter(logging.Formatter("%(message)s"))
        log.addHandler(file_hdlr)

    log.propagate = True  # bubble up to root for stdout
    return log

# ─────────────────────────────────────────────────────────────────────────────
# Structured‑log emitter (shared schema)
# ─────────────────────────────────────────────────────────────────────────────


def log_rpc(
    log: logging.Logger,
    *,
    rpc: str,
    params: Dict[str, Any],
    t_in: int,
    t_query_done: int,
    t_serialized: int,
    size_bytes: int,
) -> None:
    """Emit one JSON line with request/response timings and payload size."""
    t_out = time.time_ns()
    log.info(
        json.dumps(
            {
                "rpc": rpc,
                "params": params,
                "t_in": t_in,
                "t_query_done": t_query_done,
                "t_serialized": t_serialized,
                "t_out": t_out,
                "size_bytes": size_bytes,
                "sql_query_ns": t_query_done - t_in,
                "ser_ns": t_serialized - t_query_done,
                "app_ns": t_out - t_in,
            },
            separators=(",", ":"),
        )
    )
