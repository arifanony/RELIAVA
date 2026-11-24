# RELIAVA — next steps and open items

State as of 2026-11-24: M0–M8 are built. On top of `deb03f6` (M6-SPINE as
pushed) sit four commits: `c0d15b6` (API-first model routing, NFR-4),
`c6cc6e5` (M6 fixes), `eeb0715` (M7-TRUST), `830cc18` (M8-GATEWAY), plus
this CI/notes commit. Everything below is written so the next agent (or
the owner) can proceed without re-deriving context.

## Do these first, in order

1. **Push.** This bundle contains the full `main` history and supersedes
   the earlier `reliava-api-routing` and `reliava-m6-m8` bundles:
   `git fetch <bundle> main:refs/heads/m6-m8`, then on `main`:
   `git merge --ff-only m6-m8`, `git push`.
2. **Watch the first CI run** (`.github/workflows/ci.yml`). It runs the
   full suite on a normal runner — including the three groups that could
   not run in the build sandbox: `tests/test_sandbox.py` (needs a Docker
   daemon), the backup drill in `tests/test_repair_eval.py` (needs the
   `ai-sre-pg16` container; CI starts one), and the Temporal workflow
   test in `tests/test_spine.py` (needs `start_time_skipping`, which
   hangs in the sandbox — see item 3). Fix forward from whatever it
   reports; everything else is already green (62 passed in the sandbox's
   best full run, every file green standalone on fresh databases).
3. **Close the Genesis gates locally.** The M6–M8 gate runs could not
   complete in the build sandbox because temporalio's managed
   test-server bootstrap never returns there (recorded in the project
   as KNOWLEDGE-91c7bc14; the suites' substance was verified directly
   instead). On this machine the bootstrap works, so run, per milestone:
   `genesis gate <repo> M6-SPINE`, approve its `independent-review` gate,
   let it complete (M7-TRUST activates), then the same for M7-TRUST and
   M8-GATEWAY. Do not hand-edit `.genesis/project.json`.
4. **Run the Docker-gated tests locally once** for a second data point:
   `scripts/dev-db.sh`, then `uv run pytest tests/test_sandbox.py
   tests/test_repair_eval.py` with `DATABASE_URL` pointing at the local
   Postgres.

## Then, when keys are ready

5. **Measure, don't assume.** The SPEC numbers ($0.08 and 80k tokens per
   incident, 8-minute deadline, 80–90% token cut) are targets, not
   measurements. With a Gemini key in the shell (`GEMINI_API_KEY`,
   AC-8: shell only, never committed — this repo is public), run the
   diagnosis eval bench over 20–30 known incidents in shadow mode and
   record real cost/latency per role before changing any default model.
   Pick defaults from this repo's own eval, not public leaderboards.
6. **OpenRouter (optional).** The owner was considering a one-time $10
   credit top-up (unlocks 1,000 free-model requests/day). Undecided —
   do not treat as approved. If it happens, confirm the exact model IDs
   in the OpenRouter console before setting `DIAGNOSIS_MODEL` /
   `CRITIC_MODEL` / `TRIAGE_MODEL` specs.

## Known open items and quirks

- **Ledger pricing quirk (pre-existing, unfixed):** `cost_per_1k` only
  returns 0.0 when the model string contains "ollama", so bare local
  model names fall to the 0.001 default despite the docstring saying
  local tiers cost 0. One-line fix in `src/ai_sre/diagnosis.py` plus a
  regression test in `tests/test_model_routing.py`.
- **Resume is implemented, not yet proven end to end:** intake is
  durable (outbox + SKIP LOCKED relay) and the M6 relay now starts
  workflows, but no test has killed a process mid-diagnosis and watched
  the investigation resume. Add that test before claiming the behaviour.
- **`sources/` bloat:** ~49,000 lines of vendored reference material
  (genesis-kit + a skills library) were committed in `deb03f6`. It does
  not run, but it inflates every clone. Owner's call: keep, or remove
  and rely on the standalone genesis-kit checkout.
- **Single-host limits are by design (SPEC), not bugs:** one Postgres,
  one host, in-memory-then-PG budget records, a static runbook corpus
  (no learning loop from past incidents), a single approver role model.
  A production-scale roadmap (measured baseline → HA Postgres →
  cross-model failover → incident-memory loop) was discussed with the
  owner on 2026-11-24 but is NOT yet written into SPEC.md as M9+;
  do not start it without the owner's go-ahead.

## Working notes for the next agent

- Tests need a fresh PostgreSQL database per run; suites share one DB
  and some are order-sensitive (`tests/test_spine.py` relies on its
  alembic test running before the state-machine tests). Fixtures follow
  the pattern: `init_schema` + `init_approvals` + `init_dispatch` +
  `init_model`, then the alembic-upgrade fixture (see
  `tests/test_trust.py`) for the migration-owned columns.
- Without Docker, set `AI_SRE_SANDBOX=validate` to exercise the trust
  chain without the sandbox runner (the trust/e2e tests do this).
- Model routing is env-only: `<ROLE>_MODEL` specs are
  `gemini:` / `openrouter:` / `ollama:` (bare name = ollama), resolved
  by `transport_from_env` in `src/ai_sre/diagnosis.py`. The critic must
  be a different model family from diagnosis — the code raises if not.
- Commits on this repo are the owner's alone; if you are an agent,
  deliver work the way this bundle was delivered (local commits under
  the owner's identity, handed over for the owner to push).
