"""Validate the v1 shipment files and select unseen events for the Delta feed."""

import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Literal

EventKey = tuple[str, str, int]
MAX_POSITION = (1 << 63) - 1
MAX_WAVE_RECORDS = 10_000
MAX_WAVE_BYTES = 8 * 1024 * 1024
MAX_EVENT_BYTES = 2 * 1024
MAX_MANIFEST_BYTES = 4 * 1024
EVENT_FILE = "events.jsonl"
MANIFEST_FILE = "manifest.json"
HUBS = ("CPT", "JNB", "DBN")
STATUSES = ("CREATED", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED")
KEY_FIELDS = {"shipment_id", "effective_at", "source_position", "op"}
PAYLOAD_FIELDS = {"status", "hub", "promised_at"}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
TIMESTAMP_PATTERN = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
FIXTURE_PATTERN = re.compile(r"[a-z][a-z0-9-]{0,31}")
WAVE_PATTERN = re.compile(r"[A-Z][A-Z0-9_-]{0,31}")
SHIPMENT_PATTERN = re.compile(r"[A-Z][A-Z0-9-]{0,31}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


class ContractError(ValueError):
    """A wave failed validation before publication."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class EventConflict(ContractError):
    """A logical event key has more than one business payload."""

    def __init__(self, key: EventKey) -> None:
        super().__init__("EVENT_CONFLICT", f"Different payloads for event {key}.")
        self.key = key


def require_fresh_rebuild(*, feed_has_rows: bool, silver_exists: bool) -> None:
    """Check that a single-update rebuild starts without prior shipment state."""
    if feed_has_rows or silver_exists:
        raise ContractError(
            "REBUILD_TARGET_NOT_FRESH",
            "Use a fresh dev instance with an empty feed and no Silver table.",
        )


def canonical_json(value: object) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def utc_seconds(value: object) -> str:
    """Accept a UTC instant in the fixed seconds format."""
    if type(value) is not str or TIMESTAMP_PATTERN.fullmatch(value) is None:
        raise ContractError("INVALID_TIMESTAMP", "Expected YYYY-MM-DDTHH:MM:SSZ.")
    try:
        datetime.strptime(value, TIMESTAMP_FORMAT)
    except ValueError as error:
        raise ContractError(
            "INVALID_TIMESTAMP", "Timestamp falls outside the UTC calendar."
        ) from error
    return value


@dataclass(frozen=True)
class Event:
    shipment_id: str
    effective_at: str
    source_position: int
    op: Literal["UPSERT", "DELETE"]
    status: str | None = None
    hub: str | None = None
    promised_at: str | None = None

    @property
    def key(self) -> EventKey:
        return self.shipment_id, self.effective_at, self.source_position

    @property
    def payload_sha256(self) -> str:
        payload: dict[str, str | None] = {"op": self.op}
        if self.op == "UPSERT":
            payload.update(status=self.status, hub=self.hub, promised_at=self.promised_at)
        return sha256(canonical_json(payload)).hexdigest()

    def wire_dict(self) -> dict[str, str | int | None]:
        data = asdict(self)
        if self.op == "DELETE":
            return {field: value for field, value in data.items() if field in KEY_FIELDS}
        return data


def parse_event(value: object) -> Event:
    if not isinstance(value, dict):
        raise ContractError("INVALID_EVENT", "Each event must be a JSON object.")
    op = value.get("op")
    if op not in ("UPSERT", "DELETE"):
        raise ContractError("INVALID_OPERATION", "Operation must be UPSERT or DELETE.")
    expected_fields = KEY_FIELDS | PAYLOAD_FIELDS if op == "UPSERT" else KEY_FIELDS
    if value.keys() != expected_fields:
        raise ContractError("INVALID_FIELDS", f"Wrong fields for a {op} event.")
    shipment_id = value["shipment_id"]
    if type(shipment_id) is not str or SHIPMENT_PATTERN.fullmatch(shipment_id) is None:
        raise ContractError(
            "INVALID_SHIPMENT", "Shipment ID must use uppercase letters, digits and hyphens."
        )
    effective_at = utc_seconds(value["effective_at"])
    position = value["source_position"]
    if type(position) is not int or not 1 <= position <= MAX_POSITION:
        raise ContractError("INVALID_POSITION", "Source position must be a positive signed BIGINT.")
    if op == "DELETE":
        return Event(shipment_id, effective_at, position, "DELETE")
    status = value["status"]
    hub = value["hub"]
    if type(status) is not str or status not in STATUSES:
        raise ContractError("INVALID_STATUS", "Unknown shipment status.")
    if type(hub) is not str or hub not in HUBS:
        raise ContractError("INVALID_HUB", "Unknown shipment hub.")
    return Event(
        shipment_id,
        effective_at,
        position,
        "UPSERT",
        status,
        hub,
        utc_seconds(value["promised_at"]),
    )


@dataclass(frozen=True)
class Manifest:
    fixture_version: str
    wave: str
    file: str
    sha256: str
    record_count: int


@dataclass(frozen=True)
class Wave:
    manifest: Manifest
    events: tuple[Event, ...]


def _parse_manifest(raw: object) -> Manifest:
    fields = {"fixture_version", "wave", "file", "sha256", "record_count"}
    if not isinstance(raw, dict) or raw.keys() != fields:
        raise ContractError("INVALID_MANIFEST", "Wrong fields for the v1 manifest.")
    version, wave_id, file, digest, count = (
        raw[key] for key in ("fixture_version", "wave", "file", "sha256", "record_count")
    )
    if type(version) is not str or FIXTURE_PATTERN.fullmatch(version) is None:
        raise ContractError("INVALID_MANIFEST", "Invalid fixture version.")
    if type(wave_id) is not str or WAVE_PATTERN.fullmatch(wave_id) is None:
        raise ContractError("INVALID_MANIFEST", "Invalid wave ID.")
    if file != EVENT_FILE or type(digest) is not str or SHA256_PATTERN.fullmatch(digest) is None:
        raise ContractError("INVALID_MANIFEST", "Invalid event file name or digest.")
    if type(count) is not int or not 1 <= count <= MAX_WAVE_RECORDS:
        raise ContractError("INVALID_MANIFEST", "Record count is outside the wave limit.")
    return Manifest(version, wave_id, EVENT_FILE, digest, count)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    obj: dict[str, object] = {}
    for key, value in pairs:
        if key in obj:
            raise ContractError("DUPLICATE_JSON_FIELD", f"Repeated JSON field {key!r}.")
        obj[key] = value
    return obj


def _read_json(data: bytes) -> object:
    try:
        return json.loads(data.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise ContractError("INVALID_JSON", "Input must be valid UTF-8 JSON.") from error


def _read_limited(path: Path, byte_limit: int) -> bytes:
    try:
        with path.open("rb") as file:
            data = file.read(byte_limit + 1)
    except OSError as error:
        raise ContractError("FILE_UNAVAILABLE", f"Could not read {path.name}.") from error
    if len(data) > byte_limit:
        raise ContractError("FILE_TOO_LARGE", f"{path.name} exceeds the byte limit.")
    return data


def read_wave(
    directory: Path, *, fixture_version: str | None = None, wave_id: str | None = None
) -> Wave:
    """Check the manifest, raw bytes and every event before returning a wave."""
    wave, _ = read_wave_files(directory, fixture_version=fixture_version, wave_id=wave_id)
    return wave


def read_wave_files(
    directory: Path, *, fixture_version: str | None = None, wave_id: str | None = None
) -> tuple[Wave, dict[str, bytes]]:
    """Return the wave and the exact file bytes that passed validation."""
    manifest_data = _read_limited(directory / MANIFEST_FILE, MAX_MANIFEST_BYTES)
    manifest = _parse_manifest(_read_json(manifest_data))
    if (fixture_version is not None and manifest.fixture_version != fixture_version) or (
        wave_id is not None and manifest.wave != wave_id
    ):
        raise ContractError(
            "WRONG_WAVE", "Manifest fixture version or wave differs from the requested input."
        )
    data = _read_limited(directory / manifest.file, MAX_WAVE_BYTES)
    if sha256(data).hexdigest() != manifest.sha256:
        raise ContractError("DIGEST_MISMATCH", "Event file checksum does not match the manifest.")
    lines = data.removesuffix(b"\n").split(b"\n", manifest.record_count)
    if len(lines) != manifest.record_count:
        raise ContractError(
            "COUNT_MISMATCH", "Event file record count does not match the manifest."
        )
    events: list[Event] = []
    for position, line in enumerate(lines, 1):
        if not line.strip() or len(line) > MAX_EVENT_BYTES:
            raise ContractError("INVALID_LINE", f"Event line {position} is empty or oversized.")
        try:
            events.append(parse_event(_read_json(line)))
        except ContractError as error:
            raise ContractError(error.code, f"Event line {position}. {error}") from error
    return Wave(manifest, tuple(events)), {EVENT_FILE: data, MANIFEST_FILE: manifest_data}


@dataclass(frozen=True)
class FeedRow:
    event: Event
    first_wave: str
    source_file: str
    source_line: int

    def as_dict(self) -> dict[str, str | int | None]:
        return {
            **asdict(self.event),
            "payload_sha256": self.event.payload_sha256,
            "first_wave": self.first_wave,
            "source_file": self.source_file,
            "source_line": self.source_line,
        }


def plan_publication(
    wave: Wave,
    existing: Mapping[EventKey, str],
    *,
    source_file: str,
    expected_conflict: bool = False,
) -> tuple[FeedRow, ...]:
    """Return distinct unseen rows, or raise before the caller writes anything."""
    pending: dict[EventKey, str] = {}
    rows: list[FeedRow] = []
    for source_line, event in enumerate(wave.events, 1):
        digest = event.payload_sha256
        prior = pending.get(event.key, existing.get(event.key))
        if prior is None:
            pending[event.key] = digest
            rows.append(FeedRow(event, wave.manifest.wave, source_file, source_line))
        elif prior != digest:
            raise EventConflict(event.key)
    if expected_conflict:
        raise ContractError(
            "EXPECTED_CONFLICT_MISSING", "Wave requires a conflicting event already in the feed."
        )
    return tuple(rows)
