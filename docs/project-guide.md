# Project guide

## The reporting problem

Shipment feeds often arrive out of order. A warehouse can report an earlier transit scan after a delivery is already visible. A report that keeps only the latest status cannot answer what was true at dispatch. A report that sorts by arrival order can put the late scan after the delivery.

This project uses the effective timestamp and a source position to order each shipment history. It keeps previous versions so a reviewer can ask what the status was at a particular time. The current report also shows how many shipments arrived by their promise.

## Follow the two waves

1. Wave A publishes five events. Shipment S100 has three versions, including two at 10:00. The source position distinguishes the handover from the delivery. S200 is deleted at 10:05 and has no current row.
2. Wave B includes a late scan for S100 at 09:20. The scan closes the original created interval at 09:20 and starts a transit interval that runs until 10:00. The already recorded delivery keeps the same start.
3. S200 gets a late scan at 09:40. That changes its history before deletion. The shipment still has no status at 10:30, then comes back with a new version at 11:00 in Durban.
4. The exact duplicate in wave B adds no row. Repeating the full wave leaves the data and its normalized digest unchanged. Wave C also contains a valid new shipment, but a conflicting copy of an earlier event causes the publisher to reject the whole wave.

## How it was built

- **Contract.** The generator writes JSON Lines and a manifest with a version, count and digest. The reader verifies the original bytes, bounds the input and checks exact field types. The event key is shipment ID, effective time and source position. The payload digest is calculated from the validated event.
- **Publisher.** The job reads the named Unity Catalog volume, compares the incoming keys with the managed Delta feed and appends only unseen events. One job owns writes to the feed. The event, original line and first wave are recorded in the same append.
- **History.** Bronze reads the feed as a stream and checks sequence, operation and payload shape. Silver uses Lakeflow `AUTO CDC` with type 2 history. Its intervals include the start and exclude the end.
- **Reports.** Gold selects open intervals and counts current, delivered and on-time shipments per hub. The fixtures keep the original promise and attributes once a shipment is delivered, so the delivery version's start is the observed arrival time.
- **Verification.** The local reference builds history in Python and compares it with expected rows written separately. The native verification notebook reads the registered tables, compares the same rows and runs point-in-time queries directly against Silver.

## What the tests cover

- File or count tampering, unsupported event shape, invalid timestamps and missing sequencing fields.
- Duplicates and conflicts both within an incoming file and against keys already published.
- Two updates with a timestamp tie, differences in interval bounds, a deletion gap and recreation.
- Exact Gold counts and matching output after a replay or a rebuild from the complete accepted feed.
- Bundle configuration against the pinned CLI schema without a workspace connection.

## Assumptions and next steps

- The fixture gives every event a stable source position. A real source would need to guarantee that sequencing field across retries and replacements.
- The history describes effective time after late events have been incorporated. To answer what was known on a reporting date, keep a separate recorded-time history.
- A delivery time comes from the first version with a delivered status. Support later amendments by storing a durable delivery timestamp on the shipment.
- Test the serverless bundle in a named Unity Catalog workspace. Compare two incremental updates with a fresh reconstruction, capture update IDs and execution evidence, and inspect the lineage from feed to Gold.
