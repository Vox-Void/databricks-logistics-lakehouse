# Databricks notebook source
"""Compare registered Lakeflow outputs with the checked-in shipment history."""

import json
import re
import sys
from pathlib import Path
from uuid import UUID

from pyspark.sql import functions as F

names = ("catalog", "schema", "fixture_version", "wave", "pipeline_id", "source_root", "commit_sha")
for name in names:
    dbutils.widgets.text(name, "")
params = {name: dbutils.widgets.get(name) for name in names}
source_root = Path(params["source_root"])
if not source_root.is_absolute() or not str(source_root).startswith("/Workspace/"):
    raise ValueError("Expected an absolute Databricks Workspace file path")
sys.path.insert(0, str(source_root / "src"))

from logistics_lakehouse.reference import serving_digest

catalog, schema = params["catalog"], params["schema"]
if any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier) for identifier in (catalog, schema)):
    raise ValueError("Unexpected catalog or schema identifier")
pipeline_id = str(UUID(params["pipeline_id"]))
wave = {"A": "A", "B": "B", "AB": "B"}[params["wave"]]
spark.conf.set("spark.sql.session.timeZone", "UTC")
table = f"`{catalog}`.`{schema}`"
timestamp_format = "yyyy-MM-dd'T'HH:mm:ss'Z'"
expected = json.loads((source_root / "fixtures" / "goldens" / f"{wave}.json").read_text("utf-8"))


def normalized_history(name, limit):
    def format_bound(bound):
        return F.when(
            F.col(bound).isNotNull(),
            F.struct(
                F.date_format(f"{bound}.effective_at", timestamp_format).alias("effective_at"),
                F.col(f"{bound}.source_position").alias("source_position"),
            ),
        ).alias(bound)

    rows = (
        spark.table(f"{table}.{name}")
        .orderBy("shipment_id", "__START_AT")
        .select(
            "shipment_id",
            "status",
            "hub",
            F.date_format("promised_at", timestamp_format).alias("promised_at"),
            format_bound("__START_AT"),
            format_bound("__END_AT"),
        )
        .limit(limit + 1)
        .collect()
    )
    return [row.asDict(recursive=True) for row in rows]


def status_at(history, shipment, instant, position):
    cutpoint = F.struct(
        F.to_timestamp(F.lit(instant)).alias("effective_at"),
        F.lit(position).cast("long").alias("source_position"),
    )
    rows = (
        history.where(
            (F.col("shipment_id") == shipment)
            & (F.col("__START_AT") <= cutpoint)
            & (F.col("__END_AT").isNull() | (cutpoint < F.col("__END_AT")))
        )
        .select("status")
        .limit(2)
        .collect()
    )
    if len(rows) > 1:
        raise RuntimeError("Shipment history overlaps at the requested cutpoint")
    return rows[0].status if rows else None


def pipeline_events(publication):
    # Match the update started after this job's publisher, then read quality events for that ID.
    try:
        events = spark.sql(f"SELECT * FROM event_log('{pipeline_id}')")
        updates = (
            events.where(
                (F.col("event_type") == "create_update")
                & (
                    F.col("timestamp")
                    >= F.to_timestamp(F.lit(publication["publication_started_at"]))
                )
            )
            .select("origin.update_id")
            .distinct()
            .limit(2)
            .collect()
        )
        if len(updates) != 1:
            return {"status": "UPDATE_ID_UNAVAILABLE_OR_AMBIGUOUS"}
        update_id = updates[0].update_id
        quality = (
            events.where(
                (F.col("event_type") == "flow_progress") & (F.col("origin.update_id") == update_id)
            )
            .selectExpr(
                "origin.flow_name AS flow_name",
                "CAST(details:flow_progress.data_quality.expectations AS STRING) AS expectations",
            )
            .where("expectations IS NOT NULL")
            .limit(100)
            .collect()
        )
        return {
            "status": "FOUND",
            "update_id": update_id,
            "quality": [row.asDict(recursive=True) for row in quality],
        }
    except Exception as error:
        # Some workspaces restrict access to the pipeline's event log.
        return {"status": "EVENT_LOG_UNAVAILABLE", "error_type": type(error).__name__}


sla = (
    spark.table(f"{table}.gold_sla_by_hub")
    .orderBy("hub")
    .select(
        "hub",
        "current_shipments",
        "delivered_shipments",
        "on_time_shipments",
    )
    .limit(len(expected["sla"]) + 1)
    .collect()
)
actual = {
    "silver": normalized_history("silver_shipment_history", len(expected["silver"])),
    "current": normalized_history("gold_shipments_current", len(expected["current"])),
    "sla": [row.asDict(recursive=True) for row in sla],
}
if actual != expected:
    different = sorted(key for key in actual if actual[key] != expected[key])
    raise RuntimeError("Unexpected rows in: " + ", ".join(different))
feed_count = spark.table(f"{table}.shipment_cdc_feed").count()
if feed_count != {"A": 5, "B": 8}[wave]:
    raise RuntimeError("Unexpected number of accepted events")
history = spark.table(f"{table}.silver_shipment_history")
as_of = {
    "s100_position_40": status_at(history, "S100", "2026-01-07T10:00:00Z", 40),
    "s100_position_41": status_at(history, "S100", "2026-01-07T10:00:00Z", 41),
    "s200_delete_gap": status_at(history, "S200", "2026-01-07T10:30:00Z", (1 << 63) - 1),
}
if as_of != {
    "s100_position_40": "OUT_FOR_DELIVERY",
    "s100_position_41": "DELIVERED",
    "s200_delete_gap": None,
}:
    raise RuntimeError("Unexpected point-in-time shipment status")
publication = dbutils.jobs.taskValues.get(taskKey="publish_wave", key="publication")
evidence = {
    "kind": "NATIVE_LAKEFLOW_ROWS",
    "app_commit": params["commit_sha"],
    "pipeline_id": pipeline_id,
    "fixture_version": params["fixture_version"],
    "wave": params["wave"],
    "feed_count": feed_count,
    "serving_sha256": serving_digest(actual),
    "as_of": as_of,
    "rows": actual,
    "publication": publication,
    "pipeline_events": pipeline_events(publication),
}
dbutils.notebook.exit(json.dumps(evidence, sort_keys=True))
