"""Shared fixtures: sandbox image (built once, reused across evals)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from ai_sre.sandbox import SANDBOX_IMAGE

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def sandbox_image() -> str:
    import pytest as _pytest

    built = subprocess.run(
        [
            "docker", "build", "-q",
            "-f", "sandbox/Dockerfile",
            "--build-arg", f"PYTEST_VERSION={_pytest.__version__}",
            "-t", SANDBOX_IMAGE, ".",
        ],
        capture_output=True,
        text=True,
        timeout=300,
        cwd=ROOT,
        check=False,
    )
    assert built.returncode == 0, built.stderr
    return SANDBOX_IMAGE
