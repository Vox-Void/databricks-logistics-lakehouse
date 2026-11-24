"""Calculate expected shipment history from the ordered change feed."""

import json
from collections.abc import Iterable, Sequence
from hashlib import sha256
from pathlib import Path
from typing import TypedDict

from .contract import (
    MAX_POSITION,
    Event,
    EventConflict,
    EventKey,
    FeedRow,
    canonical_json,
    plan_publication,
    read_wave,
    utc_seconds,
)
from .fixtures import FIXTURE_ROOT, FIXTURE_VERSION, load_checks, load_golden


class SequencePoint(TypedDict):
    effective_at: str
    source_position: int


HistoryRow = TypedDict(
    "HistoryRow",
    {
        "shipment_id": str,
        "status": str,
        "hub": str,
        "promised_at": str,
        "__START_AT": SequencePoint,
        "__END_AT": SequencePoint | None,
    },
)


class SlaRow(TypedDict):
    hub: str
    current_shipments: int
    delivered_shipments: int
    on_time_shipments: int


class ServingRows(TypedDict):
    silver: list[HistoryRow]
    current: list[HistoryRow]
    sla: list[SlaRow]


class HistoryUpdate(TypedDict):
    before: HistoryRow
    after: HistoryRow


class HistoryChanges(TypedDict):
    inserted: list[HistoryRow]
    updated: list[HistoryUpdate]
    removed: list[HistoryRow]


def _point(sequence: SequencePoint) -> tuple[str, int]:
    return sequence["effective_at"], sequence["source_position"]


def _history_key(row: HistoryRow) -> EventKey:
    return row["shipment_id"], *_point(row["__START_AT"])


def shipment_history(events: Iterable[Event]) -> list[HistoryRow]:
    """Build half-open versions using every distinct change in business order."""
    rows: list[HistoryRow] = []
    active: HistoryRow | None = None
    previous: Event | None = None
    for event in sorted(events, key=lambda row: row.key):
        if previous is not None and previous.key == event.key:
            if event != previous:
                raise EventConflict(event.key)
            continue
        previous = event
        if active is not None and active["shipment_id"] != event.shipment_id:
            active = None
        if active is not None and (active["status"], active["hub"], active["promised_at"]) == (
            event.status,
            event.hub,
            event.promised_at,
        ):
            continue

        point: SequencePoint = {
            "effective_at": event.effective_at,
            "source_position": event.source_position,
        }
        if active is not None:
            active["__END_AT"] = point
            active = None
        if event.op == "UPSERT":
            if event.status is None or event.hub is None or event.promised_at is None:
                raise ValueError("UPSERT has no complete business payload.")
            active = {
                "shipment_id": event.shipment_id,
                "status": event.status,
                "hub": event.hub,
                "promised_at": event.promised_at,
                "__START_AT": point,
                "__END_AT": None,
            }
            rows.append(active)
    return rows


def verify_intervals(silver: Sequence[HistoryRow]) -> None:
    """Check interval starts, lengths, overlaps and current keys."""
    prior: HistoryRow | None = None
    for row in sorted(silver, key=_history_key):
        start = _point(row["__START_AT"])
        end = row["__END_AT"]
        if end is not None and _point(end) <= start:
            raise ValueError(f"Nonpositive interval for {row['shipment_id']} at {start}.")
        if prior is not None and prior["shipment_id"] == row["shipment_id"]:
            prior_end = prior["__END_AT"]
            if _point(prior["__START_AT"]) == start:
                raise ValueError(f"Repeated interval start for {row['shipment_id']} at {start}.")
            if prior_end is None or _point(prior_end) > start:
                raise ValueError(f"Overlapping intervals for {row['shipment_id']} at {start}.")
        prior = row


def normalized_outputs(events: Iterable[Event]) -> ServingRows:
    """Return Silver history, current shipments and the hub SLA rows."""
    silver = shipment_history(events)
    verify_intervals(silver)
    current = [row for row in silver if row["__END_AT"] is None]
    counts: dict[str, SlaRow] = {}
    for row in current:
        hub = row["hub"]
        metrics = counts.setdefault(
            hub,
            {
                "hub": hub,
                "current_shipments": 0,
                "delivered_shipments": 0,
                "on_time_shipments": 0,
            },
        )
        metrics["current_shipments"] += 1
        if row["status"] == "DELIVERED":
            metrics["delivered_shipments"] += 1
            if row["__START_AT"]["effective_at"] <= row["promised_at"]:
                metrics["on_time_shipments"] += 1
    return {"silver": silver, "current": current, "sla": [counts[hub] for hub in sorted(counts)]}


def as_of(
    silver: Sequence[HistoryRow],
    effective_at: str,
    source_position: int = MAX_POSITION,
) -> list[HistoryRow]:
    """Find shipments visible at the specified timestamp and position."""
    if type(source_position) is not int or not 1 <= source_position <= MAX_POSITION:
        raise ValueError("Cutpoint position must be a positive signed BIGINT.")
    cutpoint = (utc_seconds(effective_at), source_position)
    selected: list[HistoryRow] = []
    for row in silver:
        end = row["__END_AT"]
        if _point(row["__START_AT"]) <= cutpoint and (end is None or cutpoint < _point(end)):
            selected.append(row)
    return sorted(selected, key=_history_key)


def as_of_checks(silver: Sequence[HistoryRow]) -> dict[str, str | None]:
    def status(shipment_id: str, when: str, position: int = MAX_POSITION) -> str | None:
        return next(
            (
                row["status"]
                for row in as_of(silver, when, position)
                if row["shipment_id"] == shipment_id
            ),
            None,
        )

    return {
        "s100_at_0930": status("S100", "2026-01-07T09:30:00Z"),
        "s100_at_1000_position_40": status("S100", "2026-01-07T10:00:00Z", 40),
        "s100_at_1000_position_41": status("S100", "2026-01-07T10:00:00Z", 41),
        "s100_at_1000_timestamp_only": status("S100", "2026-01-07T10:00:00Z"),
        "s200_at_1030_delete_gap": status("S200", "2026-01-07T10:30:00Z"),
        "s200_at_1100": status("S200", "2026-01-07T11:00:00Z"),
    }


def serving_digest(rows: object) -> str:
    return sha256(canonical_json(rows)).hexdigest()


def history_changes(before: Sequence[HistoryRow], after: Sequence[HistoryRow]) -> HistoryChanges:
    old = {_history_key(row): row for row in before}
    new = {_history_key(row): row for row in after}
    return {
        "inserted": [new[key] for key in sorted(new.keys() - old.keys())],
        "updated": [
            {"before": old[key], "after": new[key]}
            for key in sorted(old.keys() & new.keys())
            if old[key] != new[key]
        ],
        "removed": [old[key] for key in sorted(old.keys() - new.keys())],
    }


def run_demo(root: Path = FIXTURE_ROOT) -> dict[str, object]:
    """Publish A, B, B again and the conflicting wave in process memory."""
    feed: dict[EventKey, FeedRow] = {}
    expected = load_checks(root)
    outputs: dict[str, ServingRows] = {}
    stages: dict[str, dict[str, object]] = {}
    for step, wave_id in (("A", "A"), ("B", "B"), ("B_replay", "B"), ("C", "C")):
        folder = root / FIXTURE_VERSION / wave_id
        wave = read_wave(folder, fixture_version=FIXTURE_VERSION, wave_id=wave_id)
        source_file = f"{FIXTURE_VERSION}/{wave_id}/{wave.manifest.file}"
        existing = {key: row.event.payload_sha256 for key, row in feed.items()}
        error_code = None
        try:
            added = plan_publication(
                wave, existing, source_file=source_file, expected_conflict=step == "C"
            )
        except EventConflict as error:
            added = ()
            error_code = error.code
        feed.update((row.event.key, row) for row in added)
        serving = normalized_outputs(row.event for row in feed.values())
        checks = as_of_checks(serving["silver"])
        golden = load_golden("A" if step == "A" else "B", root)
        if serving != golden:
            raise ValueError(f"Exact serving rows differ from the golden at step {step}.")
        state = {
            "feed_rows": len(feed),
            "new_rows": len(added),
            "history_rows": len(serving["silver"]),
            "as_of": checks,
            "error_code": error_code,
        }
        if expected[step] != state:
            raise ValueError(f"Acceptance checks differ from the golden at step {step}.")
        outputs[step] = serving
        stages[step] = {**state, "serving_sha256": serving_digest(serving)}
    return {
        "mode": "local-reference",
        "fixture_version": FIXTURE_VERSION,
        "stages": stages,
        "history_changes_A_to_B": history_changes(outputs["A"]["silver"], outputs["B"]["silver"]),
        "sla_after_B": outputs["B"]["sla"],
    }


def main() -> None:
    print(json.dumps(run_demo(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
