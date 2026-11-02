"""A deferrable file sensor: FileAvailableTrigger + operator.

Why deferrable matters: a classic FileSensor in ``poke`` mode holds a
worker slot for its entire wait — waiting for an upstream file that lands
"some time this morning" can pin a slot for hours. ``reschedule`` mode
frees the slot between pokes but still churns task instances. A
**deferrable** operator suspends itself and hands the wait to the
triggerer process, consuming no worker slot and no task slot at all; it
resumes only when the trigger fires. At fleet scale this is the difference
between sizing the worker pool for *work* and sizing it for *waiting*.

Trade-off, honestly: deferrable operators need a running triggerer and
add resume latency (seconds). For sub-minute waits, poke mode is simpler
and fine. This sensor is for the common ETL case: an upstream system drops
a file within a wide, unpredictable window.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent


class FileAvailableTrigger(BaseTrigger):
    """Fires when ``filepath`` appears on the (shared) filesystem."""

    def __init__(self, filepath: str, poll_interval: float = 30.0) -> None:
        super().__init__()
        self.filepath = filepath
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "operators.deferrable_file_sensor.FileAvailableTrigger",
            {"filepath": self.filepath, "poll_interval": self.poll_interval},
        )

    async def run(self):  # type: ignore[override]
        while True:
            if os.path.exists(self.filepath):
                yield TriggerEvent(True)
                return
            await asyncio.sleep(self.poll_interval)


class DeferrableFileSensorOperator(BaseOperator):
    """Wait for a file without holding a worker slot."""

    template_fields = ("filepath",)

    def __init__(self, *, filepath: str, poll_interval: float = 30.0,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.filepath = filepath
        self.poll_interval = poll_interval

    def execute(self, context: dict[str, Any]) -> Any:
        if os.path.exists(self.filepath):
            self.log.info("file already present: %s", self.filepath)
            return True
        self.log.info("deferring until %s appears", self.filepath)
        self.defer(
            trigger=FileAvailableTrigger(
                filepath=self.filepath, poll_interval=self.poll_interval),
            method_name="execute_complete",
        )

    def execute_complete(self, context: dict[str, Any],
                         event: Any = None) -> Any:
        if not event:
            raise AirflowFailException("file sensor resumed without an event")
        self.log.info("file detected by trigger: %s", self.filepath)
        return True
