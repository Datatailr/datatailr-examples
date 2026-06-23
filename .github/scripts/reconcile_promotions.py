#!/usr/bin/env python3
"""Reconcile the promotion manifest by diffing it against a previous revision.

The manifest (``deployments/environments.yaml``) declares the desired job
version per environment::

    pre:
      "Price Server": 3
    prod:
      "Price Server": 2

This script compares the current manifest against an older Git revision and
turns every ADDED or CHANGED entry into a ``dt job promote`` command, following
the chain ``dev -> pre -> prod`` (a version landing in ``pre`` is promoted from
``dev``; a version landing in ``prod`` is promoted from ``pre``).

Modes:
  --plan                 Print the promotion plan (no changes). Used by the PR
                         check so reviewers can see exactly what will happen.
  --apply --target ENV   Execute the promotions for a single target environment.
                         Used on merge to main, behind environment approvals.

The previous manifest revision is read from Git via ``--old-ref`` (e.g. the PR
base branch, or the push "before" commit). If the ref or file is missing, the
old manifest is treated as empty (so every declared entry is considered new).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from dataclasses import dataclass

import yaml

# Each environment is promoted FROM the previous one in the chain.
SOURCE_OF = {"pre": "dev", "prod": "pre"}
# Apply order matters: a version must reach `pre` before it can reach `prod`.
TARGET_ORDER = ["pre", "prod"]


@dataclass(frozen=True)
class Promotion:
    target: str
    job: str
    version: int
    source: str


def load_manifest_text(text: str) -> dict[str, dict[str, int]]:
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise SystemExit("Manifest must be a mapping of environment -> {job: version}.")
    result: dict[str, dict[str, int]] = {}
    for env in TARGET_ORDER:
        section = data.get(env) or {}
        if not isinstance(section, dict):
            raise SystemExit(f"Manifest section '{env}' must be a mapping of job -> version.")
        parsed: dict[str, int] = {}
        for job, version in section.items():
            try:
                parsed[str(job)] = int(version)
            except (TypeError, ValueError):
                raise SystemExit(
                    f"Version for '{job}' under '{env}' must be an integer, got {version!r}."
                )
        result[env] = parsed
    return result


def read_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def read_old_manifest(manifest_path: str, old_ref: str | None) -> dict[str, dict[str, int]]:
    if not old_ref:
        return load_manifest_text("")
    proc = subprocess.run(
        ["git", "show", f"{old_ref}:{manifest_path}"],
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        # Ref or file does not exist at that revision -> treat as empty baseline.
        return load_manifest_text("")
    return load_manifest_text(proc.stdout)


def compute_promotions(
    old: dict[str, dict[str, int]],
    new: dict[str, dict[str, int]],
) -> list[Promotion]:
    promotions: list[Promotion] = []
    for env in TARGET_ORDER:
        old_env = old.get(env, {})
        new_env = new.get(env, {})
        for job, version in new_env.items():
            if old_env.get(job) != version:  # added or changed
                promotions.append(
                    Promotion(target=env, job=job, version=version, source=SOURCE_OF[env])
                )
    return promotions


def render_plan(promotions: list[Promotion]) -> str:
    lines = ["## Promotion plan", ""]
    if not promotions:
        lines.append("No promotions detected (manifest unchanged).")
        return "\n".join(lines) + "\n"
    lines += [
        "| Target | Job | Version | Promote from | Command |",
        "|---|---|---|---|---|",
    ]
    for p in promotions:
        cmd = f"`dt job promote \"{p.job}\" {p.version} {p.source}`"
        lines.append(f"| {p.target} | {p.job} | {p.version} | {p.source} | {cmd} |")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_github_output(promotions: list[Promotion]) -> None:
    out_path = os.environ.get("GITHUB_OUTPUT")
    if not out_path:
        return
    has = {env: any(p.target == env for p in promotions) for env in TARGET_ORDER}
    with open(out_path, "a", encoding="utf-8") as fh:
        for env in TARGET_ORDER:
            fh.write(f"has_{env}={'true' if has[env] else 'false'}\n")


def apply_promotions(promotions: list[Promotion], target: str) -> int:
    selected = [p for p in promotions if p.target == target]
    if not selected:
        print(f"Nothing to promote to '{target}'.")
        return 0

    failures = 0
    for p in selected:
        print(f"::group::Promote '{p.job}' v{p.version} from {p.source} -> {p.target}")
        # Show the current state of the source before promoting (best-effort).
        subprocess.run(["dt", "job", "versions", p.job, "-e", p.source])
        result = subprocess.run(
            ["dt", "job", "promote", p.job, str(p.version), p.source]
        )
        if result.returncode != 0:
            print(f"::error::Failed to promote '{p.job}' v{p.version} from {p.source}.")
            failures += 1
        else:
            # Confirm the version landed in the target environment.
            subprocess.run(["dt", "job", "versions", p.job, "-e", p.target])
        print("::endgroup::")

    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        default="deployments/environments.yaml",
        help="Path to the promotion manifest (default: deployments/environments.yaml).",
    )
    parser.add_argument(
        "--old-ref",
        default=None,
        help="Git ref to read the previous manifest from (e.g. origin/main or a commit SHA).",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true", help="Print the promotion plan only.")
    mode.add_argument("--apply", action="store_true", help="Execute promotions for --target.")
    parser.add_argument(
        "--target",
        choices=TARGET_ORDER,
        help="Target environment to apply promotions to (required with --apply).",
    )
    args = parser.parse_args(argv)

    if args.apply and not args.target:
        parser.error("--apply requires --target {pre|prod}.")

    new_manifest = load_manifest_text(read_file(args.manifest))
    old_manifest = read_old_manifest(args.manifest, args.old_ref)
    promotions = compute_promotions(old_manifest, new_manifest)

    if args.plan:
        sys.stdout.write(render_plan(promotions))
        write_github_output(promotions)
        return 0

    return apply_promotions(promotions, args.target)


if __name__ == "__main__":
    raise SystemExit(main())
