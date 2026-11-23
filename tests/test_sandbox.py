"""M3 eval: patch candidates (FR-7) verified in a locked-down sandbox (FR-8)."""

from __future__ import annotations

import hashlib
import tempfile
from pathlib import Path
from typing import Any

import pytest

from ai_sre.patches import PatchCandidate, apply_patch, propose_patches
from ai_sre.sandbox import run_in_sandbox

CALC_BUGGY = '''def apply_discount(total, coupon):
    if coupon == "SAVE10":
        return total + total * 0.10
    return total
'''

TEST_CALC = '''from calc import apply_discount


def test_repro_coupon_bug():
    assert apply_discount(100.0, "SAVE10") == 90.0


def test_no_coupon_full_price():
    assert apply_discount(100.0, None) == 100.0
'''

NET_PROBE = "python -c \"import socket; socket.create_connection(('8.8.8.8', 53), timeout=5)\""
ENV_DUMP = "env"
LIMITS_PROBE = "cat /sys/fs/cgroup/pids.max && cat /sys/fs/cgroup/memory.max"


def make_target() -> Any:
    tmp = tempfile.TemporaryDirectory(prefix="m3-target-")
    Path(tmp.name, "calc.py").write_text(CALC_BUGGY)
    Path(tmp.name, "test_calc.py").write_text(TEST_CALC)
    return tmp


def test_patch_digest_and_guards() -> None:
    good = PatchCandidate(
        path="calc.py",
        old="return total + total * 0.10",
        new="return total - total * 0.10",
        base_revision="m3-fixture-v1",
    )
    assert good.digest == hashlib.sha256((good.diff + "m3-fixture-v1").encode()).hexdigest()
    assert "-return total + total * 0.10" in good.diff
    assert "+return total - total * 0.10" in good.diff
    assert good.diff.startswith("--- a/calc.py")
    assert propose_patches([good], (".",)) == [good]
    with pytest.raises(ValueError):
        propose_patches([good] * 4, (".",))
    for bad_path in ("/etc/passwd", "../escape.py", "sub/../../x.py"):
        with pytest.raises(ValueError):
            propose_patches(
                [PatchCandidate(path=bad_path, old="a", new="b", base_revision="r")],
                (".",),
            )
    with pytest.raises(ValueError):
        propose_patches(
            [PatchCandidate(path="calc.py", old="same", new="same", base_revision="r")],
            (".",),
        )


def test_repro_fails_before_passes_after(sandbox_image: str) -> None:
    _ = sandbox_image
    with make_target() as target:
        before = run_in_sandbox(target, "python -m pytest test_calc.py -x -q")
        assert before.exit_code != 0
        assert "test_repro_coupon_bug" in (before.stdout + before.stderr)
        candidate = PatchCandidate(
            path="calc.py",
            old="return total + total * 0.10",
            new="return total - total * 0.10",
            base_revision="m3-fixture-v1",
        )
        propose_patches([candidate], (".",))
        apply_patch(target, candidate)
        after = run_in_sandbox(target, "python -m pytest test_calc.py -q")
        assert after.exit_code == 0, after.stdout + after.stderr
        assert "2 passed" in after.stdout


def test_sandbox_has_no_network(sandbox_image: str) -> None:
    _ = sandbox_image
    with make_target() as target:
        probed = run_in_sandbox(target, NET_PROBE)
    assert probed.exit_code != 0


def test_sandbox_has_no_credentials(sandbox_image: str) -> None:
    _ = sandbox_image
    with make_target() as target:
        dumped = run_in_sandbox(target, ENV_DUMP)
    assert dumped.exit_code == 0
    for secret in ("OPENROUTER_API_KEY", "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY"):
        assert secret not in dumped.stdout


def test_sandbox_resource_limits(sandbox_image: str) -> None:
    _ = sandbox_image
    with make_target() as target:
        probed = run_in_sandbox(target, LIMITS_PROBE)
    assert probed.exit_code == 0, probed.stderr
    pids_max, memory_max = probed.stdout.split()
    assert pids_max == "256"
    assert memory_max == str(2 * 1024**3)
