# Databricks notebook source
"""Validate a landed wave and append any new event keys to the Delta feed."""

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F

for name in (
    "catalog",
    "schema",
    "landing_path",
    "fixture_version",
    "wave",
    "source_root",
    "commit_sha",
):
    dbutils.widgets.text(name, "")
params = {
    name: dbutils.widgets.get(name)
    for name in (
        "catalog",
        "schema",
        "landing_path",
        "fixture_version",
        "wave",
        "source_root",
        "commit_sha",
    )
}
source_root = Path(params["source_root"])
if not source_root.is_absolute() or not str(source_root).startswith("/Workspace/"):
    raise ValueError("Expected an absolute Databricks Workspace file path")
sys.path.insert(0, str(source_root / "src"))

from logistics_lakehouse.contract import ContractError, plan_publication, read_wave

catalog, schema = params["catalog"], params["schema"]
if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier) for identifier in (catalog, schema)):
    raise ValueError("Unexpected catalog or schema identifier")
landing = Path(params["landing_path"])
if landing != Path(f"/Volumes/{catalog}/{schema}/landing"):
    raise ValueError("Landing volume does not match the selected catalog and schema")
fixture = params["fixture_version"]
wave = params["wave"]
if fixture != "logistics-v1" or wave not in {"A", "B", "C", "AB"}:
    raise ValueError("Unsupported fixture or wave")

feed = f"`{catalog}`.`{schema}`.`shipment_cdc_feed`"
spark.conf.set("spark.sql.session.timeZone", "UTC")
keys = ("shipment_id", "effective_at", "source_position")
feed_schema = """
    shipment_id STRING, effective_at TIMESTAMP, source_position BIGINT, op STRING,
    status STRING, hub STRING, promised_at TIMESTAMP, payload_sha256 STRING,
    first_wave STRING, source_file STRING, source_line INT
"""
timestamp_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"


def publish(wave_id):
    source = landing / fixture / wave_id
    incoming = read_wave(source, fixture_version=fixture, wave_id=wave_id)
    event_keys = sorted({row.key for row in incoming.events})
    batches = spark.createDataFrame(
        [
            (shipment, datetime.fromisoformat(when), position)
            for shipment, when, position in event_keys
        ],
        "shipment_id STRING, effective_at TIMESTAMP, source_position BIGINT",
    )
    known = (
        spark.table(feed)
        .join(batches, list(keys))
        .select(
            "shipment_id",
            F.date_format("effective_at", timestamp_format).alias("effective_at"),
            "source_position",
            "payload_sha256",
        )
        .limit(len(event_keys) + 1)
        .collect()
    )
    previous = {
        (row.shipment_id, row.effective_at, row.source_position): row.payload_sha256
        for row in known
    }
    if len(previous) != len(known):
        raise RuntimeError("Feed contains more than one row for the same event key")
    proposed = plan_publication(
        incoming, previous, source_file=str(source / incoming.manifest.file)
    )

    # One append records the accepted event and its publication provenance together.
    if proposed:
        new_rows = []
        for row in proposed:
            values = row.as_dict()
            values["effective_at"] = datetime.fromisoformat(values["effective_at"])
            if values["promised_at"] is not None:
                values["promised_at"] = datetime.fromisoformat(values["promised_at"])
            new_rows.append(values)
        spark.createDataFrame(new_rows, feed_schema).write.mode("append").saveAsTable(feed)
    published = spark.table(feed)
    return {
        "wave": wave_id,
        "file_sha256": incoming.manifest.sha256,
        "raw_count": incoming.manifest.record_count,
        "appended_this_attempt": len(proposed),
        "first_published_in_wave": published.where(F.col("first_wave") == wave_id).count(),
        "feed_count": published.count(),
    }


publication_started_at = datetime.now(UTC).isoformat()
try:
    reports = [publish(wave_id) for wave_id in ("A", "B")] if wave == "AB" else [publish(wave)]
except ContractError as error:
    print(json.dumps({"wave": wave, "result": "REJECTED", "reason": error.code}))
    raise RuntimeError(f"Wave {wave} rejected: {error.code}") from None

output = {
    "kind": "NATIVE_DELTA_PUBLICATION",
    "fixture_version": fixture,
    "app_commit": params["commit_sha"],
    "publication_started_at": publication_started_at,
    "waves": reports,
}
dbutils.jobs.taskValues.set(key="publication", value=output)
dbutils.notebook.exit(json.dumps(output, sort_keys=True))
