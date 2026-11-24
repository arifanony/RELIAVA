"""FR-8: locked-down sandbox execution for patch verification.

No network, no credentials, non-root user, read-only source mount with a
writable 1 GiB tmpfs workspace, 2 CPUs, 2 GiB RAM, 256 PIDs, 10 min cap.
Docker Desktop on Mac cannot do user-namespace remapping, so "rootless" is
realized as: numeric non-root user + no-new-privileges + read-only rootfs.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass

SANDBOX_IMAGE = "ai-sre-sandbox:latest"
SANDBOX_TIMEOUT_S = 600

_LIMITS = [
    "--network", "none",
    "--user", "65534:65534",
    "--security-opt", "no-new-privileges",
    "--read-only",
    "--tmpfs", "/tmp:rw,exec,size=1g",
    "--cpus", "2",
    "--memory", "2g",
    "--pids-limit", "256",
]

_SAFE_ENV = [
    "PYTHONDONTWRITEBYTECODE=1",
    "PYTEST_ADDOPTS=-p no:cacheprovider",
]


@dataclass
class SandboxResult:
    exit_code: int
    stdout: str
    stderr: str


def _grant_container_read(tree: str) -> None:
    """Grant other-read on a sandbox source tree.

    The tree is bind-mounted read-only and read inside the container as uid
    65534. Linux bind mounts keep host ownership, so temp dirs (mode 0700)
    are unreadable there; Docker Desktop is lenient, which is why this only
    fails on Linux CI runners.
    """
    for root, dirs, files in os.walk(tree):
        os.chmod(root, os.stat(root).st_mode | 0o055)
        for name in dirs + files:
            path = os.path.join(root, name)
            if os.path.islink(path):
                continue
            mode = os.stat(path).st_mode
            extra = 0o044 | (0o011 if mode & 0o100 else 0)
            os.chmod(path, mode | extra)


def run_in_sandbox(
    source_dir: str,
    command: str,
    timeout_s: int = SANDBOX_TIMEOUT_S,
    image: str = SANDBOX_IMAGE,
) -> SandboxResult:
    """Copy source_dir (mounted read-only) to tmpfs and run command there."""
    _grant_container_read(source_dir)
    shell = f"cp -r /src/. /tmp/work/ && cd /tmp/work && {command}"
    completed = subprocess.run(
        [
            "docker", "run", "--rm",
            *_LIMITS,
            *(e for pair in (("--env", v) for v in _SAFE_ENV) for e in pair),
            "--volume", f"{source_dir}:/src:ro",
            "--workdir", "/tmp",
            image, "sh", "-c", shell,
        ],
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )
    return SandboxResult(
        exit_code=completed.returncode,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )
