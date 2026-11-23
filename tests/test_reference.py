import json
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from logistics_lakehouse.contract import (
    EVENT_FILE,
    MANIFEST_FILE,
    MAX_POSITION,
    ContractError,
    EventConflict,
    EventKey,
    Wave,
    parse_event,
    plan_publication,
    read_wave,
)
from logistics_lakehouse.fixtures import (
    FIXTURE_ROOT,
    FIXTURE_VERSION,
    fixture_files,
    generate,
    load_golden,
)
from logistics_lakehouse.reference import (
    as_of,
    as_of_checks,
    history_changes,
    normalized_outputs,
    run_demo,
    verify_intervals,
)


def wave(name: str, root: Path = FIXTURE_ROOT) -> Wave:
    return read_wave(root / FIXTURE_VERSION / name, fixture_version=FIXTURE_VERSION, wave_id=name)


def publish_input(batch: Wave, seen: dict[EventKey, str]) -> list[tuple[str, int]]:
    rows = plan_publication(batch, seen, source_file=f"{batch.manifest.wave}/{EVENT_FILE}")
    seen.update((row.event.key, row.event.payload_sha256) for row in rows)
    return [(row.first_wave, row.source_line) for row in rows]


def test_regeneration_matches_committed_bytes(tmp_path: Path) -> None:
    generate(tmp_path)
    for filename, contents in fixture_files().items():
        assert (
            (FIXTURE_ROOT / filename).read_bytes() == (tmp_path / filename).read_bytes() == contents
        )


@pytest.mark.parametrize(
    "mutation,code",
    [
        ({"record_count": 4}, "COUNT_MISMATCH"),
        ({"sha256": "0" * 64}, "DIGEST_MISMATCH"),
        ({"file": "../events.jsonl"}, "INVALID_MANIFEST"),
        ({"wave": "B"}, "WRONG_WAVE"),
        ({"fixture_version": "other-v1"}, "WRONG_WAVE"),
        ({"record_count": True}, "INVALID_MANIFEST"),
    ],
)
def test_manifest_integrity(tmp_path: Path, mutation: dict[str, object], code: str) -> None:
    generate(tmp_path)
    path = tmp_path / FIXTURE_VERSION / "A" / MANIFEST_FILE
    manifest = json.loads(path.read_bytes())
    path.write_text(json.dumps({**manifest, **mutation}), encoding="utf-8")
    with pytest.raises(ContractError) as result:
        wave("A", tmp_path)
    assert result.value.code == code


@pytest.mark.parametrize(
    "field,value,code",
    [
        ("source_position", 0, "INVALID_POSITION"),
        ("source_position", True, "INVALID_POSITION"),
        ("source_position", -1, "INVALID_POSITION"),
        ("source_position", 1.0, "INVALID_POSITION"),
        ("source_position", MAX_POSITION + 1, "INVALID_POSITION"),
        ("effective_at", "2026-01-07T09:00:00+00:00", "INVALID_TIMESTAMP"),
        ("effective_at", "2026-01-07T09:00:00.000Z", "INVALID_TIMESTAMP"),
        ("effective_at", "2026-02-30T09:00:00Z", "INVALID_TIMESTAMP"),
        ("effective_at", "2026-01-07T09:00:00z", "INVALID_TIMESTAMP"),
        ("op", "INSERT", "INVALID_OPERATION"),
        ("status", "Unknown", "INVALID_STATUS"),
        ("hub", "BLO", "INVALID_HUB"),
        ("promised_at", None, "INVALID_TIMESTAMP"),
        ("shipment_id", "S100\n", "INVALID_SHIPMENT"),
        ("schema_version", 2, "INVALID_FIELDS"),
    ],
)
def test_event_contract_exact_types_and_shapes(field: str, value: object, code: str) -> None:
    original = wave("A").events[0].wire_dict()
    with pytest.raises(ContractError) as result:
        parse_event({**original, field: value})
    assert result.value.code == code


def test_missing_sequence_and_delete_payload_are_invalid() -> None:
    original = wave("A").events[0].wire_dict()
    original.pop("source_position")
    with pytest.raises(ContractError, match="Wrong fields"):
        parse_event(original)
    delete = wave("A").events[-1]
    assert set(delete.wire_dict()) == {"shipment_id", "effective_at", "source_position", "op"}
    with pytest.raises(ContractError, match="Wrong fields"):
        parse_event({**delete.wire_dict(), "status": None})


def test_repeated_json_properties_fail_even_with_valid_manifest(tmp_path: Path) -> None:
    generate(tmp_path)
    directory = tmp_path / FIXTURE_VERSION / "A"
    event_path = directory / EVENT_FILE
    raw = event_path.read_bytes().replace(
        b'"source_position":10', b'"source_position":10,"source_position":21', 1
    )
    event_path.write_bytes(raw)
    manifest_path = directory / MANIFEST_FILE
    manifest = json.loads(manifest_path.read_bytes())
    manifest_path.write_text(
        json.dumps({**manifest, "sha256": sha256(raw).hexdigest()}), encoding="utf-8"
    )
    with pytest.raises(ContractError, match="Repeated JSON field") as result:
        wave("A", tmp_path)
    assert result.value.code == "DUPLICATE_JSON_FIELD"


def test_payload_digest_ignores_json_member_order() -> None:
    event = wave("A").events[0]
    original = event.wire_dict()
    reversed_fields = dict(reversed(tuple(original.items())))
    assert parse_event(reversed_fields).payload_sha256 == event.payload_sha256


def test_publish_deduplicates_and_keeps_file_line_numbers() -> None:
    seen: dict[EventKey, str] = {}
    a, b = wave("A"), wave("B")
    assert publish_input(a, seen) == [("A", line) for line in range(1, 6)]
    assert publish_input(b, seen) == [("B", 1), ("B", 3), ("B", 4)]
    assert publish_input(b, seen) == []
    assert len(seen) == 8
    repeated = replace(a, events=(a.events[0], a.events[0]))
    assert publish_input(repeated, {}) == [("A", 1)]
    delete_feed = plan_publication(a, {}, source_file=f"A/{EVENT_FILE}")[-1].as_dict()
    assert delete_feed["promised_at"] is None
    assert delete_feed["source_line"] == 5


def test_conflicts_leave_feed_unchanged_even_after_a_valid_new_key() -> None:
    seen: dict[EventKey, str] = {}
    publish_input(wave("A"), seen)
    publish_input(wave("B"), seen)
    snapshot = seen.copy()
    c = wave("C")
    for order in (c.events, tuple(reversed(c.events))):
        with pytest.raises(EventConflict) as result:
            publish_input(replace(c, events=order), seen)
        assert result.value.key == ("S100", "2026-01-07T10:00:00Z", 41)
        assert seen == snapshot
        assert "S300" not in {key[0] for key in seen}
    original = wave("A").events[2]
    with pytest.raises(EventConflict):
        publish_input(replace(c, events=(original, *c.events)), {})


def test_reference_demo_matches_history_and_sla_goldens() -> None:
    report = run_demo()
    stages = report["stages"]
    assert isinstance(stages, dict)
    assert stages["A"]["feed_rows"] == 5
    assert stages["B"]["feed_rows"] == 8
    assert stages["B_replay"]["new_rows"] == 0
    assert stages["C"]["error_code"] == "EVENT_CONFLICT"
    assert (
        stages["B"]["serving_sha256"]
        == stages["B_replay"]["serving_sha256"]
        == stages["C"]["serving_sha256"]
    )
    before = load_golden("A")["silver"]
    after = load_golden("B")["silver"]
    changes = history_changes(before, after)
    assert len(changes["inserted"]) == 3
    assert len(changes["updated"]) == 2
    assert changes["removed"] == []


def test_full_reconstruction_and_boundary_queries() -> None:
    a, b = wave("A"), wave("B")
    outputs = normalized_outputs(reversed(a.events + b.events))
    assert outputs == load_golden("B")
    silver = outputs["silver"]
    assert as_of_checks(silver) == {
        "s100_at_0930": "IN_TRANSIT",
        "s100_at_1000_position_40": "OUT_FOR_DELIVERY",
        "s100_at_1000_position_41": "DELIVERED",
        "s100_at_1000_timestamp_only": "DELIVERED",
        "s200_at_1030_delete_gap": None,
        "s200_at_1100": "CREATED",
    }
    assert [row["shipment_id"] for row in as_of(silver, "2026-01-07T10:05:00Z", 49)] == [
        "S100",
        "S200",
    ]
    assert [row["shipment_id"] for row in as_of(silver, "2026-01-07T10:05:00Z", 50)] == ["S100"]


def test_invariants_reject_duplicate_starts_nonpositive_bounds_and_overlaps() -> None:
    original = load_golden("B")["silver"]
    with pytest.raises(ValueError, match="Repeated interval start"):
        verify_intervals([*original, original[0]])
    with pytest.raises(ValueError, match="Nonpositive interval"):
        verify_intervals([{**original[0], "__END_AT": original[0]["__START_AT"]}])
    with pytest.raises(ValueError, match="Overlapping intervals"):
        verify_intervals([{**original[0], "__END_AT": None}, *original[1:]])


def test_same_payload_needs_no_new_version_and_arrival_on_promise_counts() -> None:
    original = wave("A").events[2]
    exactly_due = replace(original, effective_at="2026-01-07T10:30:00Z")
    duplicate_status = replace(exactly_due, source_position=42)
    outputs = normalized_outputs([duplicate_status, exactly_due])
    assert len(outputs["silver"]) == 1
    assert outputs["silver"][0]["__START_AT"]["source_position"] == 41
    assert outputs["sla"][0]["on_time_shipments"] == 1
