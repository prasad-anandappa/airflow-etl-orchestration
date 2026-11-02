"""Warehouse loading helpers shared by the DAGs.

The demo warehouse is SQLite (zero-infrastructure, so the DAGs run in the
compose stack and the functions unit-test without services). The load
semantics are the part that matters and they transfer unchanged to a real
warehouse MERGE:

* **Idempotent by batch**: each load is keyed by (table, logical_date) in a
  ``_load_ledger`` table with a content hash — re-running a date replaces
  nothing and duplicates nothing; it verifies and no-ops.
* **Upsert on the natural key**: rows merge on the pipeline's key columns,
  so a corrected re-delivery converges.
"""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

Record = dict[str, Any]


def read_csv_records(path: str | Path) -> list[Record]:
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def content_hash(records: list[Record]) -> str:
    canonical = json.dumps(records, sort_keys=True, default=str,
                           separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


class Warehouse:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS _load_ledger (
                   table_name TEXT, logical_date TEXT, content_hash TEXT,
                   rows_written INTEGER,
                   PRIMARY KEY (table_name, logical_date))"""
        )
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def load(self, table: str, records: list[Record], *,
             logical_date: str, key_columns: list[str]) -> dict[str, Any]:
        """Idempotently load records. Returns a small report dict."""
        digest = content_hash(records)
        prior = self._conn.execute(
            "SELECT content_hash FROM _load_ledger "
            "WHERE table_name = ? AND logical_date = ?",
            (table, logical_date),
        ).fetchone()
        if prior and prior[0] == digest:
            return {"status": "skipped_duplicate", "rows": 0, "hash": digest}
        if prior and prior[0] != digest:
            raise ValueError(
                f"{table}@{logical_date} already loaded with different "
                "contents — refusing to silently overwrite; clear the "
                "ledger entry deliberately if this is a correction.")

        if records:
            columns = sorted({k for r in records for k in r})
            self._conn.execute(
                f'CREATE TABLE IF NOT EXISTS "{table}" '
                f'({", ".join(chr(34) + c + chr(34) for c in columns)}, '
                f'PRIMARY KEY ({", ".join(chr(34) + c + chr(34) for c in key_columns)}))'
            )
            sql = (
                f'INSERT INTO "{table}" ({", ".join(chr(34) + c + chr(34) for c in columns)}) '
                f'VALUES ({", ".join("?" for _ in columns)}) '
                f'ON CONFLICT ({", ".join(chr(34) + c + chr(34) for c in key_columns)}) '
                f'DO UPDATE SET ' + ", ".join(
                    f'"{c}" = excluded."{c}"' for c in columns
                    if c not in key_columns)
            )
            with self._conn:
                self._conn.executemany(
                    sql, [tuple(r.get(c) for c in columns) for r in records])
                self._conn.execute(
                    "INSERT OR REPLACE INTO _load_ledger VALUES (?, ?, ?, ?)",
                    (table, logical_date, digest, len(records)))
        return {"status": "loaded", "rows": len(records), "hash": digest}

    def count(self, table: str) -> int:
        try:
            return int(self._conn.execute(
                f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])
        except sqlite3.OperationalError:
            return 0
