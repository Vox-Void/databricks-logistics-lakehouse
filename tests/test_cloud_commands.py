import runpy
import subprocess
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
OPTIONS = ["--target", "dev", "--profile", "logistics", "--catalog", "workspace"]


@pytest.fixture
def cloud() -> dict[str, Any]:
    entrypoint = runpy.run_path(str(ROOT / "scripts" / "cloud"))["main"]
    return entrypoint.__globals__


class FakeDatabricks:
    def __init__(self) -> None:
        self.commands: list[tuple[str, ...]] = []
        self.files: dict[str, bytes] = {}

    def call(self, *command: str, **kwargs: bool) -> subprocess.CompletedProcess[str]:
        self.commands.append(command)
        if command[:2] != ("fs", "cp"):
            return subprocess.CompletedProcess(command, 0, "", "")
        source, destination = command[2:4]
        if source.startswith("dbfs:"):
            Path(destination).write_bytes(self.files[source])
            return subprocess.CompletedProcess(command, 0, "", "")
        assert command[4] == "--overwrite=false"
        if destination in self.files:
            return subprocess.CompletedProcess(command, 1, "", "already exists")
        self.files[destination] = Path(source).read_bytes()
        return subprocess.CompletedProcess(command, 0, "", "")

    def read_json(self, *command: str) -> dict[str, str] | list[Any]:
        self.commands.append(command)
        if command[:2] == ("volumes", "read"):
            return {"volume_type": "MANAGED"}
        if command[:2] in (("schemas", "list"), ("pipelines", "list-pipelines")):
            return []
        return {}


@pytest.fixture
def cli(cloud: dict[str, Any], monkeypatch: pytest.MonkeyPatch) -> FakeDatabricks:
    fake = FakeDatabricks()
    monkeypatch.setitem(
        cloud, "connect", lambda profile: (fake, "https://example.invalid", "PM@example.invalid")
    )
    monkeypatch.setitem(cloud, "source_revision", lambda: "a" * 40)
    return fake


def test_preflight_only_reads_workspace_metadata(
    cloud: dict[str, Any], cli: FakeDatabricks
) -> None:
    assert cloud["main"](["preflight", *OPTIONS]) == 0
    assert [command[:2] for command in cli.commands] == [
        ("catalogs", "get"),
        ("schemas", "list"),
        ("pipelines", "list-pipelines"),
        ("bundle", "validate"),
    ]


def test_deploy_validates_the_same_target_first(cloud: dict[str, Any], cli: FakeDatabricks) -> None:
    assert cloud["main"](["deploy", *OPTIONS]) == 0
    validate, deploy = cli.commands
    assert validate[:2] == ("bundle", "validate")
    assert deploy[:2] == ("bundle", "deploy")
    assert validate[2:] == deploy[2:]
    assert "catalog=workspace" in deploy
    assert f"schema={cloud['dev_schema']('PM@example.invalid', None)}" in deploy


def test_rebuild_requires_an_isolated_dev_instance(
    cloud: dict[str, Any], cli: FakeDatabricks
) -> None:
    with pytest.raises(SystemExit) as exc:
        cloud["main"](["run", *OPTIONS, "--wave", "AB"])
    assert exc.value.code == 2
    assert not cli.commands
    assert cloud["main"](["run", *OPTIONS, "--wave", "AB", "--dev-instance", "rebuild"]) == 0
    (run,) = cli.commands
    assert run[:3] == ("bundle", "run", "logistics_wave")
    assert run[-2:] == ("--params", "wave=AB")
    assert f"schema={cloud['dev_schema']('PM@example.invalid', 'rebuild')}" in run


def test_staging_replays_exact_bytes_and_refuses_modified_events(
    cloud: dict[str, Any], cli: FakeDatabricks
) -> None:
    command = ["stage", *OPTIONS, "--wave", "A"]
    assert cloud["main"](command) == 0
    assert [key.rsplit("/", 1)[1] for key in cli.files] == ["events.jsonl", "manifest.json"]
    assert cloud["main"](command) == 0

    event_file = next(path for path in cli.files if path.endswith("/events.jsonl"))
    cli.files[event_file] = b"different bytes"
    cli.commands.clear()
    assert cloud["main"](command) == 1
    assert all("manifest.json" not in " ".join(commands) for commands in cli.commands)
    assert cli.files[event_file] == b"different bytes"
