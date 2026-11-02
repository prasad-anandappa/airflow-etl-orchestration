"""Alerting hooks shared by all DAGs in this project.

Two channels, in order of escalation:

1. **Incident file** — every failure/SLA miss appends a JSON record to
   ``$INCIDENT_LOG`` (default ``/tmp/airflow_incidents.jsonl`` inside the
   workers). This is the channel that always works, including when the
   network doesn't, and it gives the on-call a machine-readable trail.
2. **Webhook** — if ``$ALERT_WEBHOOK_URL`` is set (Slack/Teams incoming
   webhook), a short message is POSTed. Best-effort: a dead webhook must
   never fail the callback itself, so errors are logged, not raised.

Callbacks in Airflow run in the scheduler/worker context and must be
importable and fast — keep them that way (no retries with sleeps here).
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

INCIDENT_LOG = os.environ.get("INCIDENT_LOG", "/tmp/airflow_incidents.jsonl")
WEBHOOK_URL = os.environ.get("ALERT_WEBHOOK_URL", "")


def _record(kind: str, context: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    ti = context.get("ti")
    incident = {
        "kind": kind,
        "at": datetime.now(timezone.utc).isoformat(),
        "dag_id": getattr(ti, "dag_id", context.get("dag", None) and context["dag"].dag_id),
        "task_id": getattr(ti, "task_id", None),
        "run_id": context.get("run_id"),
        "logical_date": str(context.get("logical_date")),
        **extra,
    }
    try:
        with open(INCIDENT_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(incident, default=str) + "\n")
    except OSError as exc:  # incident log unwritable: scream in the task log
        log.error("could not write incident log: %s", exc)
    if WEBHOOK_URL:
        try:
            payload = json.dumps(
                {"text": f":rotating_light: Airflow {kind}: "
                         f"{incident['dag_id']}.{incident['task_id']} "
                         f"run {incident['run_id']} — {extra.get('detail', '')}"}
            ).encode()
            req = urllib.request.Request(
                WEBHOOK_URL, data=payload,
                headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5)  # noqa: S310 - operator-configured URL
        except Exception as exc:  # webhook is best-effort by design
            log.warning("alert webhook failed: %s", exc)
    return incident


def on_failure_callback(context: dict[str, Any]) -> None:
    """DAG/task failure hook: record + alert, never raise."""
    exc = context.get("exception")
    _record("task_failure", context, {"detail": repr(exc) if exc else "unknown"})


def on_sla_miss_callback(dag, task_list, blocking_task_list,
                         slas, blocking_tis) -> None:  # Airflow SLA signature
    """SLA miss hook (Airflow calls this from the scheduler)."""
    context: dict[str, Any] = {"dag": dag, "run_id": None, "logical_date": None}
    _record("sla_miss", context,
            {"detail": f"tasks past SLA: {[t.task_id for t in task_list]}"})
