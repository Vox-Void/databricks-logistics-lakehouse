# Databricks Logistics Lakehouse

Reconstruct a shipment's status after events arrive late, and check whether it met its promised delivery time. The example uses synthetic shipments for Cape Town, Johannesburg and Durban.

The first wave creates five feed events. A later wave inserts three new events, including a transit scan that splits an existing history interval. An exact duplicate adds zero rows. A conflicting wave fails as a unit so its unrelated new shipment stays out of the feed.

Watch the [2:06 walkthrough](docs/demo.mp4) for the local reference run.

## Run the local checks

Use Python 3.11.15, uv 0.13.9 and Databricks CLI 1.27.0. The CLI generates a bundle schema locally. These commands use synthetic files and need no workspace or credentials.

```bash
./scripts/dev bootstrap
./scripts/dev test
./scripts/dev demo
```

The demo writes `artifacts/local-reference.json`. It compares exact shipment history, current shipments and SLA counts with the independently written expected rows in `fixtures/goldens/`.

## Run on Databricks

The bundle contains a serverless job with a publisher, an incremental Lakeflow pipeline task and a verifier. The native pipeline uses Unity Catalog and the `AUTO CDC` API. Follow the [cloud runbook](docs/cloud-runbook.md) to select a workspace and a target.

The published evidence covers the Python reference model and bundle schema. The repository includes the native workflow for a workspace run.

The [project guide](docs/project-guide.md) follows a late scan through the feed, history and Gold views.
