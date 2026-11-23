-- Bind catalog and schema to the names reported by cloud preflight.
USE CATALOG IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema);
USE SCHEMA IDENTIFIER(:schema);

-- The landing volume contains the original manifests and events.
CREATE VOLUME IF NOT EXISTS landing;

-- Each feed row contains both the event and its publication record.
CREATE TABLE IF NOT EXISTS shipment_cdc_feed (
    shipment_id STRING NOT NULL,
    effective_at TIMESTAMP NOT NULL,
    source_position BIGINT NOT NULL,
    op STRING NOT NULL,
    status STRING,
    hub STRING,
    promised_at TIMESTAMP,
    payload_sha256 STRING NOT NULL,
    first_wave STRING NOT NULL,
    source_file STRING NOT NULL,
    source_line INT NOT NULL
)
USING DELTA
TBLPROPERTIES ('delta.appendOnly' = 'true');
