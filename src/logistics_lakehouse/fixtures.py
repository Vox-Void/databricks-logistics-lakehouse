"""Generate three shipment waves and the independently specified expected rows."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, cast

from .contract import EVENT_FILE, MANIFEST_FILE, canonical_json, parse_event

if TYPE_CHECKING:
    from .reference import HistoryRow, SequencePoint, ServingRows, SlaRow

FIXTURE_ROOT = Path(__file__).resolve().parents[2] / "fixtures"
FIXTURE_VERSION = "logistics-v1"
DATE = "2026-01-07"


def _timestamp(clock: str) -> str:
    return f"{DATE}T{clock}:00Z"


def _upsert(
    shipment_id: str,
    clock: str,
    position: int,
    status: str,
    hub: str,
    promised: str,
) -> dict[str, str | int]:
    return {
        "shipment_id": shipment_id,
        "effective_at": _timestamp(clock),
        "source_position": position,
        "op": "UPSERT",
        "status": status,
        "hub": hub,
        "promised_at": _timestamp(promised),
    }


def _wave_events() -> dict[str, list[dict[str, str | int]]]:
    delivered = _upsert("S100", "10:00", 41, "DELIVERED", "CPT", "10:30")
    return {
        "A": [
            _upsert("S100", "09:00", 10, "CREATED", "CPT", "10:30"),
            _upsert("S100", "10:00", 40, "OUT_FOR_DELIVERY", "CPT", "10:30"),
            delivered,
            _upsert("S200", "09:05", 11, "CREATED", "JNB", "12:00"),
            {
                "shipment_id": "S200",
                "effective_at": _timestamp("10:05"),
                "source_position": 50,
                "op": "DELETE",
            },
        ],
        "B": [
            _upsert("S100", "09:20", 30, "IN_TRANSIT", "CPT", "10:30"),
            delivered,
            _upsert("S200", "09:40", 35, "IN_TRANSIT", "JNB", "12:00"),
            _upsert("S200", "11:00", 60, "CREATED", "DBN", "13:00"),
        ],
        "C": [
            {**delivered, "promised_at": _timestamp("10:35")},
            _upsert("S300", "11:10", 70, "CREATED", "JNB", "14:00"),
        ],
    }


def _point(value: tuple[str, int]) -> SequencePoint:
    clock, position = value
    return {"effective_at": _timestamp(clock), "source_position": position}


def _row(
    shipment_id: str,
    status: str,
    hub: str,
    promised: str,
    start: tuple[str, int],
    end: tuple[str, int] | None,
) -> HistoryRow:
    return {
        "shipment_id": shipment_id,
        "status": status,
        "hub": hub,
        "promised_at": _timestamp(promised),
        "__START_AT": _point(start),
        "__END_AT": _point(end) if end is not None else None,
    }


def _expected_outputs() -> dict[str, ServingRows]:
    # The boundaries below come from the fixture brief.
    s100_out = _row("S100", "OUT_FOR_DELIVERY", "CPT", "10:30", ("10:00", 40), ("10:00", 41))
    s100_delivered = _row("S100", "DELIVERED", "CPT", "10:30", ("10:00", 41), None)
    s200_recreated = _row("S200", "CREATED", "DBN", "13:00", ("11:00", 60), None)
    cpt: SlaRow = {
        "hub": "CPT",
        "current_shipments": 1,
        "delivered_shipments": 1,
        "on_time_shipments": 1,
    }
    return {
        "A": {
            "silver": [
                _row("S100", "CREATED", "CPT", "10:30", ("09:00", 10), ("10:00", 40)),
                s100_out,
                s100_delivered,
                _row("S200", "CREATED", "JNB", "12:00", ("09:05", 11), ("10:05", 50)),
            ],
            "current": [s100_delivered],
            "sla": [cpt],
        },
        "B": {
            "silver": [
                _row("S100", "CREATED", "CPT", "10:30", ("09:00", 10), ("09:20", 30)),
                _row("S100", "IN_TRANSIT", "CPT", "10:30", ("09:20", 30), ("10:00", 40)),
                s100_out,
                s100_delivered,
                _row("S200", "CREATED", "JNB", "12:00", ("09:05", 11), ("09:40", 35)),
                _row("S200", "IN_TRANSIT", "JNB", "12:00", ("09:40", 35), ("10:05", 50)),
                s200_recreated,
            ],
            "current": [s100_delivered, s200_recreated],
            "sla": [
                cpt,
                {
                    "hub": "DBN",
                    "current_shipments": 1,
                    "delivered_shipments": 0,
                    "on_time_shipments": 0,
                },
            ],
        },
    }


def _expected_checks() -> dict[str, object]:
    as_of_a = {
        "s100_at_0930": "CREATED",
        "s100_at_1000_position_40": "OUT_FOR_DELIVERY",
        "s100_at_1000_position_41": "DELIVERED",
        "s100_at_1000_timestamp_only": "DELIVERED",
        "s200_at_1030_delete_gap": None,
        "s200_at_1100": None,
    }
    as_of_b = {**as_of_a, "s100_at_0930": "IN_TRANSIT", "s200_at_1100": "CREATED"}
    stage_a = {
        "feed_rows": 5,
        "new_rows": 5,
        "history_rows": 4,
        "as_of": as_of_a,
        "error_code": None,
    }
    stage_b = {
        "feed_rows": 8,
        "new_rows": 3,
        "history_rows": 7,
        "as_of": as_of_b,
        "error_code": None,
    }
    stage_replay = {**stage_b, "new_rows": 0}
    return {
        "A": stage_a,
        "B": stage_b,
        "B_replay": stage_replay,
        "C": {**stage_replay, "error_code": "EVENT_CONFLICT"},
    }


def load_golden(wave: str, root: Path = FIXTURE_ROOT) -> ServingRows:
    if wave not in {"A", "B"}:
        raise ValueError("Serving goldens are available for A and B.")
    return cast("ServingRows", json.loads((root / "goldens" / f"{wave}.json").read_bytes()))


def load_checks(root: Path = FIXTURE_ROOT) -> dict[str, object]:
    return cast(dict[str, object], json.loads((root / "goldens" / "checks.json").read_bytes()))


def _pretty(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, indent=2) + "\n").encode("utf-8")


def fixture_files() -> dict[Path, bytes]:
    files = {
        Path("goldens") / f"{wave}.json": _pretty(rows)
        for wave, rows in _expected_outputs().items()
    }
    files[Path("goldens/checks.json")] = _pretty(_expected_checks())
    for wave, events in _wave_events().items():
        directory = Path(FIXTURE_VERSION) / wave
        data = b"".join(canonical_json(parse_event(raw).wire_dict()) + b"\n" for raw in events)
        manifest = {
            "fixture_version": FIXTURE_VERSION,
            "wave": wave,
            "file": EVENT_FILE,
            "sha256": sha256(data).hexdigest(),
            "record_count": len(events),
        }
        files[directory / EVENT_FILE] = data
        files[directory / MANIFEST_FILE] = _pretty(manifest)
    return files


def generate(root: Path = FIXTURE_ROOT) -> None:
    for relative, contents in fixture_files().items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(contents)


def main() -> None:
    generate()
    print(f"Generated shipment fixture waves in {FIXTURE_ROOT}")


if __name__ == "__main__":
    main()
