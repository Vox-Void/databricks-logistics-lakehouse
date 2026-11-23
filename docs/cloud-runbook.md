# Run the logistics job on Databricks

The local tests check the same fixture contract and expected rows as the cloud verifier. A Databricks workspace is required to run the two native updates and capture Unity Catalog lineage.

## Choose a workspace and profile

- Start with Databricks Free Edition for the synthetic fixture. Check that the workspace has serverless Jobs, Lakeflow Spark Declarative Pipelines and an existing Unity Catalog catalog where you can create a schema. Free Edition limits concurrent work and can pause compute when its quota is used. [Free Edition limits](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- Put Databricks CLI v1.27.0 on your PATH. The bundle requires this exact version. Use the release checksums to verify the installer. [CLI release](https://github.com/databricks/cli/releases/tag/v1.27.0)
- Sign in on your machine with a named workspace profile. Select the workspace URL shown for the workspace you intend to use. [CLI authentication](https://docs.databricks.com/aws/en/dev-tools/cli/authentication)

```sh
databricks auth login --host "<workspace-url>" --profile logistics-dev
databricks current-user me --profile logistics-dev
./scripts/cloud preflight --target dev --profile logistics-dev --catalog workspace
```

Replace `workspace` with the catalog you selected. If you use PowerShell, run `python scripts/cloud` in place of `./scripts/cloud`. Preflight reads workspace metadata, shows the target catalog and schema, and validates the bundle. Check those names before continuing. Workspace permissions and available compute quota determine whether the first job can run.

## Create the landing volume and feed

Run [bootstrap.sql](../db/bootstrap.sql) in a Databricks SQL editor or notebook on current Databricks compute. In the editor, bind the string parameters `catalog` and `schema` to the names printed by preflight. The schema may be absent on the first preflight.

For a personal dev workspace, the signed-in user deploys and runs the Job and pipeline. That user needs `USE CATALOG` and `CREATE SCHEMA` on the catalog for setup. To create and run the workflow in an existing schema, the principal needs `USE SCHEMA`, the permissions to create streaming tables and materialized views, `SELECT` and `MODIFY` on the feed, and `READ VOLUME` on the landing volume. The profile staging files also needs `WRITE VOLUME`. Grant readers `USE CATALOG`, `USE SCHEMA` and `SELECT` on both Gold objects after the first update. Confirm each grant in the actual workspace. [Unity Catalog with pipelines](https://docs.databricks.com/aws/en/ldp/unity-catalog), [Unity Catalog privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference)

## Deploy and process the waves

Each stage command copies the committed event file first, confirms the stored bytes, then copies and confirms the manifest. Stage can be repeated after an interrupted upload. It reuses byte-for-byte matching files and stops if a file differs.

```sh
./scripts/cloud deploy --target dev --profile logistics-dev --catalog workspace
./scripts/cloud stage --target dev --profile logistics-dev --catalog workspace --wave A
./scripts/cloud run --target dev --profile logistics-dev --catalog workspace --wave A
./scripts/cloud stage --target dev --profile logistics-dev --catalog workspace --wave B
./scripts/cloud run --target dev --profile logistics-dev --catalog workspace --wave B
./scripts/cloud run --target dev --profile logistics-dev --catalog workspace --wave B
./scripts/cloud stage --target dev --profile logistics-dev --catalog workspace --wave C
./scripts/cloud run --target dev --profile logistics-dev --catalog workspace --wave C
```

- After A, check that the feed has five rows and Silver has four intervals.
- After B, check that the feed has eight rows and Silver has seven intervals. Save the comparison report and the Silver and Gold digests.
- The B replay should publish zero events and return the same digests.
- C should fail in `publish_wave`. In the Job run, confirm that `update_lakehouse` and `verify_wave` were skipped. Query the feed, `silver_shipment_history`, `gold_shipments_current` and `gold_sla_by_hub` in the selected schema and compare their rows with B. The feed must still contain eight rows and no `S300`. Use [assertions.sql](../db/assertions.sql) to recheck the delete gap and the two versions sharing a timestamp.

The same source files support both `dev` and `release`. For a release run, use `--target release`, a profile for the intended workspace, `--schema` for its isolated schema, `--release-root` for its dedicated shared workspace directory, and `--release-service-principal` for the execution identity's application ID. Follow the same preflight and bootstrap steps before deploying. Give the service principal the runtime privileges described above and confirm that the deployer can select that identity. [Bundle run identities](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as)

## Reconstruct the same history

Archive the incremental reports before starting this check. On Free Edition, use one active ETL pipeline at a time. If the quota blocks creating the isolated pipeline, confirm that the incremental evidence has been exported before retiring the previous disposable dev pipeline in the workspace.

Repeat preflight, bootstrap and deploy with `--dev-instance rebuild`. That option selects a different schema and deployment state. For that same instance, stage A and B separately, then run `AB` once. The publisher loads both waves before the single native update. Compare its final Silver and Gold rows and digests with the saved B report from the incremental run.

```sh
./scripts/cloud preflight --target dev --profile logistics-dev --catalog workspace --dev-instance rebuild
# Run bootstrap.sql with the schema from this preflight.
./scripts/cloud deploy --target dev --profile logistics-dev --catalog workspace --dev-instance rebuild
./scripts/cloud stage --target dev --profile logistics-dev --catalog workspace --dev-instance rebuild --wave A
./scripts/cloud stage --target dev --profile logistics-dev --catalog workspace --dev-instance rebuild --wave B
./scripts/cloud run --target dev --profile logistics-dev --catalog workspace --dev-instance rebuild --wave AB
```

The publisher requires an empty feed and an absent Silver table in the selected schema. If `AB` fails after either wave writes rows, start the reconstruction again with a fresh dev instance name. Bootstrap its schema, deploy, and stage both waves there before running `AB`.

## Save evidence from the run

- Record the Git commit, CLI version, workspace tier and the redacted validate and deploy transcript. Deploy from a clean checkout so the job report can record the revision.
- Keep each Job run ID and its pipeline update ID. The Jobs UI shows notebook reports. For command-line exports, use `databricks jobs get-run <run-id>` and `databricks jobs get-run-output <notebook-task-run-id>` with the same profile. Use the notebook task run ID for the task report.
- Export the exact Silver and Gold rows, the replay comparison, the C conflict and the reconstruction comparison. Capture the event log for each native update under its pipeline run identity. Record quality counts when reported and keep the error for a failed expectation. [Pipeline event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs)
- Capture the visible named-table lineage in Catalog Explorer. Record execution time and usage when the workspace exposes them. [Unity Catalog lineage](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage)

Before sharing the evidence, redact private workspace URLs, user identities and any configuration values. Keep the synthetic shipment records, object names, statuses and normalized row comparisons visible.
