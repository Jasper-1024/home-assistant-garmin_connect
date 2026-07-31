#!/usr/bin/env python3
"""Offline metadata, dependency, and candidate-identity gate for beta releases."""

from __future__ import annotations

import argparse
import importlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path

ROOT = Path(__file__).parents[1]
BETA_VERSION = re.compile(
    r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)-beta\.(?:0|[1-9][0-9]*)"
)
HOME_ASSISTANT_VERSION = re.compile(
    r"[1-9][0-9]*\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)"
)
REQUIREMENT = re.compile(r"(?P<name>[A-Za-z0-9][A-Za-z0-9_.-]*)==(?P<version>[^\s;]+)$")
IMPORT_NAMES = {"ha-garmin": "ha_garmin", "garmin-fit-sdk": "garmin_fit_sdk"}
FULL_COMMIT_SHA = re.compile(r"[0-9A-Fa-f]{40}(?:[0-9A-Fa-f]{24})?")


class GateError(RuntimeError):
    """Raised when release input is inconsistent or incomplete."""


@dataclass(frozen=True)
class ReleaseMetadata:
    """Release values shared by package metadata and runtime checks."""

    version: str
    home_assistant: str
    requirements: dict[str, str]
    manifest_requirements: dict[str, str]


def _read_pinned_requirements(path: Path) -> dict[str, str]:
    """Return exact distribution pins, ignoring comments and HA's floor."""
    pins: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or line.startswith("homeassistant"):
            continue
        match = REQUIREMENT.fullmatch(line)
        if match:
            pins[match["name"].lower()] = match["version"]
    return pins


def read_release_metadata(root: Path = ROOT) -> ReleaseMetadata:
    """Validate local release metadata without contacting package registries."""
    manifest = json.loads(
        (root / "custom_components" / "garmin_connect" / "manifest.json").read_text()
    )
    hacs = json.loads((root / "hacs.json").read_text())
    version = manifest.get("version")
    floor = hacs.get("homeassistant")
    if not isinstance(version, str) or not BETA_VERSION.fullmatch(version):
        raise GateError("manifest version must be a semantic -beta.N prerelease")
    if not isinstance(floor, str) or not HOME_ASSISTANT_VERSION.fullmatch(floor):
        raise GateError("hacs homeassistant must be a canonical stable version")

    requirements_file = (root / "requirements.txt").read_text().splitlines()
    if f"homeassistant>={floor}" not in requirements_file:
        raise GateError("requirements Home Assistant floor does not match hacs")
    pins = _read_pinned_requirements(root / "requirements.txt")
    manifest_requirements = manifest.get("requirements")
    if not isinstance(manifest_requirements, list):
        raise GateError("manifest requirements must be a list")
    manifest_pins: dict[str, str] = {}
    for requirement in manifest_requirements:
        if not isinstance(requirement, str):
            raise GateError("manifest requirement must be a string")
        match = REQUIREMENT.fullmatch(requirement)
        if not match:
            raise GateError(f"manifest requirement is not an exact pin: {requirement!r}")
        if pins.get(match["name"].lower()) != match["version"]:
            raise GateError(f"requirements.txt is missing manifest pin: {requirement}")
        manifest_pins[match["name"].lower()] = match["version"]
    return ReleaseMetadata(version, floor, pins, manifest_pins)


def _installed_version(distribution: str, expected: str) -> str:
    """Return one installed distribution version or a concise gate failure."""
    try:
        return metadata.version(distribution)
    except metadata.PackageNotFoundError as err:
        raise GateError(
            f"required distribution {distribution} is not installed; "
            f"expected {distribution}=={expected}"
        ) from err


def _require_import(distribution: str, module: str, expected: str) -> object:
    """Import one required module or turn dependency errors into a gate failure."""
    try:
        return importlib.import_module(module)
    except ImportError as err:
        raise GateError(
            f"required module {module} for distribution {distribution} cannot be imported; "
            f"expected {distribution}=={expected}"
        ) from err


def check_installed_requirements(release: ReleaseMetadata) -> None:
    """Require every manifest dependency and exact Core to be importable locally."""
    for distribution, expected in release.manifest_requirements.items():
        installed = _installed_version(distribution, expected)
        if installed != expected:
            raise GateError(f"{distribution}=={installed} installed; expected {expected}")
        _require_import(
            distribution,
            IMPORT_NAMES.get(distribution, distribution.replace("-", "_")),
            expected,
        )
    home_assistant = _installed_version("homeassistant", release.home_assistant)
    if home_assistant != release.home_assistant:
        raise GateError(
            f"homeassistant=={home_assistant} installed; expected {release.home_assistant}"
        )
    const = _require_import("homeassistant", "homeassistant.const", release.home_assistant)
    if getattr(const, "__version__", None) != release.home_assistant:
        raise GateError(
            "homeassistant.const.__version__ does not match the release floor; "
            f"expected homeassistant=={release.home_assistant}"
        )


def _git(root: Path, *args: str) -> str:
    """Return one Git value or raise a concise gate error."""
    result = subprocess.run(
        ["git", *args], cwd=root, text=True, capture_output=True, check=False
    )
    if result.returncode:
        raise GateError(result.stderr.strip() or "Git release identity check failed")
    return result.stdout.strip()


def check_candidate_identity(
    release: ReleaseMetadata, candidate_sha: str, release_tag: str, release_version: str
) -> None:
    """Bind the clean checkout HEAD, annotated tag, and Release version together."""
    if release_tag != release.version or release_version != release.version:
        raise GateError("tag and Release version must equal manifest semantic prerelease")
    if not BETA_VERSION.fullmatch(release_tag):
        raise GateError("release tag must be a semantic -beta.N prerelease")
    if not FULL_COMMIT_SHA.fullmatch(candidate_sha):
        raise GateError("candidate SHA must be a full commit SHA (40 or 64 characters)")
    if _git(ROOT, "status", "--porcelain"):
        raise GateError("candidate identity gate requires a clean worktree")
    candidate_commit = _git(ROOT, "rev-parse", "--verify", f"{candidate_sha}^{{commit}}")
    if candidate_sha.lower() != candidate_commit.lower():
        raise GateError("candidate SHA must name a commit directly")
    head_commit = _git(ROOT, "rev-parse", "--verify", "HEAD^{commit}")
    if candidate_commit != head_commit:
        raise GateError("candidate SHA does not match the current HEAD commit")
    tag_ref = f"refs/tags/{release_tag}"
    if _git(ROOT, "cat-file", "-t", tag_ref) != "tag":
        raise GateError("release tag must be an annotated tag, not a lightweight tag")
    tagged_commit = _git(ROOT, "rev-parse", "--verify", f"{tag_ref}^{{commit}}")
    if tagged_commit != candidate_commit:
        raise GateError("release tag does not resolve to the candidate SHA and current HEAD")


def main(argv: list[str] | None = None) -> int:
    """Run metadata/install checks locally, with optional final tag identity checks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check-installed", action="store_true")
    parser.add_argument(
        "--candidate-sha",
        metavar="FULL_COMMIT_SHA",
        help="full 40- or 64-character commit SHA that exactly matches checkout HEAD",
    )
    parser.add_argument(
        "--release-tag",
        help="annotated tag name for the candidate (defaults to manifest version)",
    )
    parser.add_argument(
        "--release-version",
        help="GitHub Release version for the candidate (defaults to manifest version)",
    )
    args = parser.parse_args(argv)
    try:
        release = read_release_metadata()
        if args.check_installed:
            check_installed_requirements(release)
        identity_requested = any(
            (args.candidate_sha, args.release_tag, args.release_version)
        )
        if identity_requested and not args.candidate_sha:
            raise GateError(
                "candidate SHA must be supplied before a tag or Release version"
            )
        if args.candidate_sha:
            check_candidate_identity(
                release,
                args.candidate_sha,
                args.release_tag or release.version,
                args.release_version or release.version,
            )
    except GateError as err:
        print(f"release gate failed: {err}", file=sys.stderr)
        return 1
    print("release gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
