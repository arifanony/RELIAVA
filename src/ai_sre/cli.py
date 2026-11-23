"""Operator CLI for FR-9 approvals: request, grant, reject, show."""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

from ai_sre.approvals import decide, find_valid, init_approvals, request_approval
from ai_sre.db import get_pool


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


def cmd_request(args: argparse.Namespace) -> int:
    async def scenario() -> dict[str, Any]:
        pool = get_pool()
        await pool.open()
        try:
            await init_approvals(pool)
            approval = await request_approval(
                pool, args.digest, args.target, args.revision, args.policy, args.approver
            )
        finally:
            await pool.close()
        return {"id": approval.id, "expires_at": approval.expires_at.isoformat()}

    print(json.dumps(_run(scenario())))
    return 0


def cmd_decide(args: argparse.Namespace, grant: bool) -> int:
    async def scenario() -> dict[str, Any]:
        pool = get_pool()
        await pool.open()
        try:
            await init_approvals(pool)
            approval = await decide(pool, args.id, grant)
        finally:
            await pool.close()
        return {
            "id": approval.id,
            "decision": approval.decision,
            "valid_until": approval.valid_until.isoformat() if approval.valid_until else None,
        }

    print(json.dumps(_run(scenario())))
    return 0


def cmd_show(args: argparse.Namespace) -> int:
    async def scenario() -> dict[str, Any] | None:
        pool = get_pool()
        await pool.open()
        try:
            await init_approvals(pool)
            approval = await find_valid(pool, args.digest, args.target, args.revision)
        finally:
            await pool.close()
        if approval is None:
            return None
        return {
            "id": approval.id,
            "approver": approval.approver,
            "valid_until": approval.valid_until.isoformat() if approval.valid_until else None,
        }

    print(json.dumps(_run(scenario())))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ai-sre", description="AI SRE operator CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    req = sub.add_parser("request", help="request approval for an action digest")
    req.add_argument("--digest", required=True)
    req.add_argument("--target", required=True)
    req.add_argument("--revision", required=True)
    req.add_argument("--policy", required=True)
    req.add_argument("--approver", required=True)
    grant = sub.add_parser("grant", help="grant a pending approval")
    grant.add_argument("--id", type=int, required=True)
    reject = sub.add_parser("reject", help="reject a pending approval")
    reject.add_argument("--id", type=int, required=True)
    show = sub.add_parser("show", help="show the valid approval for an action")
    show.add_argument("--digest", required=True)
    show.add_argument("--target", required=True)
    show.add_argument("--revision", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "request":
        return cmd_request(args)
    if args.command == "grant":
        return cmd_decide(args, grant=True)
    if args.command == "reject":
        return cmd_decide(args, grant=False)
    if args.command == "show":
        return cmd_show(args)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
