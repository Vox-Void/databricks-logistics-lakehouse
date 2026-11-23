"""Check the bundle's YAML against the schema shipped by the pinned CLI."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]
from jsonschema import Draft202012Validator  # type: ignore[import-untyped]

ROOT = Path(__file__).resolve().parents[1]


def load_bundle() -> dict[str, Any]:
    bundle: dict[str, Any] = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    resources: dict[str, dict[str, Any]] = bundle.setdefault("resources", {})
    # Collect the resource definitions referenced by the root bundle.
    for pattern in bundle["include"]:
        paths = sorted(ROOT.glob(pattern))
        if not paths:
            raise ValueError(f"No bundle files match {pattern}.")
        for path in paths:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
            for category, entries in document["resources"].items():
                combined = resources.setdefault(category, {})
                duplicates = combined.keys() & entries.keys()
                if duplicates:
                    raise ValueError(
                        f"{path.name} redefines {category} {', '.join(sorted(duplicates))}."
                    )
                combined.update(entries)
    return bundle


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cli", default=shutil.which("databricks"), help="Databricks CLI executable."
    )
    args = parser.parse_args()
    if not args.cli:
        parser.error("Databricks CLI is required. Put it on PATH or pass --cli.")

    bundle = load_bundle()
    expected = bundle["bundle"]["databricks_cli_version"]
    version = subprocess.check_output([args.cli, "--version"], text=True, encoding="utf-8").strip()
    if version != f"Databricks CLI v{expected}":
        raise SystemExit(f"Expected Databricks CLI v{expected}. Found {version}.")
    schema_output = subprocess.check_output(
        [args.cli, "bundle", "schema"], text=True, encoding="utf-8"
    )
    validator = Draft202012Validator(json.loads(schema_output))
    problems = list(validator.iter_errors(bundle))
    for problem in problems:
        # The CLI wraps object schemas in unions. Show the nested error as well.
        details = problem.context or [problem]
        for detail in details:
            print(f"{detail.json_path} {detail.message}", file=sys.stderr)
    if problems:
        return 1
    print(f"Bundle YAML matches the Databricks CLI v{expected} schema.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
