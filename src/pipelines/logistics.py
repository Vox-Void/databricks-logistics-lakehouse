"""Native Lakeflow tables for shipment history and current delivery performance."""

from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, expr, struct, when
from pyspark.sql.functions import sum as total

source = (
    f"{spark.conf.get('logistics.catalog')}.{spark.conf.get('logistics.schema')}.shipment_cdc_feed"
)


@dp.table(name="bronze_shipment_cdc", table_properties={"pipelines.reset.allowed": "false"})
@dp.expect_all_or_fail(
    {
        "required_sequence": "shipment_id IS NOT NULL AND effective_at IS NOT NULL AND source_position > 0",
        "known_operation": "op IN ('UPSERT', 'DELETE')",
        "valid_payload": """(
            op = 'UPSERT'
            AND status IN ('CREATED', 'IN_TRANSIT', 'OUT_FOR_DELIVERY', 'DELIVERED')
            AND hub IN ('CPT', 'JNB', 'DBN')
            AND promised_at IS NOT NULL
        ) OR (
            op = 'DELETE' AND status IS NULL AND hub IS NULL AND promised_at IS NULL
        )""",
        "source_provenance": "payload_sha256 RLIKE '^[0-9a-f]{64}$' AND first_wave IS NOT NULL AND source_file IS NOT NULL AND source_line > 0",
    }
)
def bronze_shipment_cdc():
    return spark.readStream.table(source).select(
        "shipment_id",
        "effective_at",
        "source_position",
        "op",
        "status",
        "hub",
        "promised_at",
        "payload_sha256",
        "first_wave",
        "source_file",
        "source_line",
    )


dp.create_streaming_table(
    name="silver_shipment_history",
    table_properties={
        "pipelines.cdc.tombstoneGCThresholdInSeconds": "604800",
        "pipelines.reset.allowed": "false",
    },
)

# The timestamp and source position together determine the version order.
dp.create_auto_cdc_flow(
    target="silver_shipment_history",
    source="bronze_shipment_cdc",
    keys=["shipment_id"],
    sequence_by=struct("effective_at", "source_position"),
    apply_as_deletes=expr("op = 'DELETE'"),
    except_column_list=[
        "op",
        "effective_at",
        "source_position",
        "payload_sha256",
        "first_wave",
        "source_file",
        "source_line",
    ],
    track_history_column_list=["status", "hub", "promised_at"],
    stored_as_scd_type=2,
)


@dp.materialized_view(name="gold_shipments_current")
def gold_shipments_current():
    return spark.read.table("silver_shipment_history").where(col("__END_AT").isNull())


@dp.materialized_view(name="gold_sla_by_hub")
def gold_sla_by_hub():
    current = spark.read.table("gold_shipments_current")
    delivered = col("status") == "DELIVERED"
    on_time = delivered & (col("__START_AT.effective_at") <= col("promised_at"))
    return current.groupBy("hub").agg(
        count("*").alias("current_shipments"),
        total(when(delivered, 1).otherwise(0)).alias("delivered_shipments"),
        total(when(on_time, 1).otherwise(0)).alias("on_time_shipments"),
    )
