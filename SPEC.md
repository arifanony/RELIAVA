# Product specification — AI SRE

> Status: draft. A coding agent must not implement product code until this specification is approved through Genesis.

## Problem

Engineers receive monitoring alerts but investigation and remediation are slow, manual, and depend on scarce expert attention: evidence is scattered across systems, diagnoses are debated under pressure, and fixes applied by hand can cause second outages. The product turns a monitoring alert into a verified recovery — or an honest escalation — without ever letting a model act on production by itself. Full rationale: Reference Decisions and Layers 1–13 in `ai-sre-presentation-edition.pdf`.

## Users

- On-call engineer of a single engineering organization (single tenant): acknowledges incidents, reviews diagnoses, approves or rejects actions, receives escalations.
- Service owner / approver: authorizes exact changes; in the showcase the same human plays both roles, exercising RBAC by role rather than by person.
- Affected users of the target Python web service benefit indirectly through faster verified recovery.

## Functional requirements

- FR-1: Accept alerts via `POST /v1/incidents` with `Idempotency-Key`; insert incident plus outbox row plus `NOTIFY` in one PostgreSQL transaction and return 202.
- FR-2: Deduplicate repeat alerts; identify service and environment; start Temporal workflow `incident-<id>` via the embedded `LISTEN/NOTIFY` relay (2 s `SKIP LOCKED` fallback).
- FR-3: Collect read-only evidence scoped to service and window (logs, metrics, traces, deployment history, config diff, git revision); pre-process with Drain3 log clustering and Python AST traceback slicing into a context of at most 8k tokens where every item carries source, window, and content hash.
- FR-4: Generate hypotheses from evidence; every claim cites stored evidence IDs validated against the database.
- FR-5: Produce a cited diagnosis reviewed by a cross-family critic (at most 2 revisions); enforce 20 evidence tool calls, 3 attempts per skill step, and 25 s per model call.
- FR-6: On inconclusive evidence, budget exhaustion, or deadline expiry, record INCONCLUSIVE with observed and missing evidence, propose no change, and escalate.
- FR-7: Propose at most 3 patch candidates per diagnosis as unified diffs on allowed paths only, each identified by SHA-256 digest over diff plus base revision.
- FR-8: Test each candidate in a rootless sandbox (no network, no credentials; 2 CPU, 2 GiB RAM, 256 PIDs, 1 GiB tmpfs, 10 min): reproduction fails before and passes after, plus the full suite is green.
- FR-9: Request approval with expiry 24 h; grant or reject via CLI and HTTP API binding action digest, target environment, source revision, policy version, approver, and 2 h validity.
- FR-10: Dispatch only APPROVED actions from the trusted worker holding the sole staging write credentials; recheck digest and revision on reload; mark timeouts UNKNOWN and reconcile via the target ledger — never auto-retry.
- FR-11: Verify independently over a 10 min window (error rate below 1%, p95 within 20% of baseline, original failing request replayed); close RESOLVED or escalate.
- FR-12: Keep every evidence item, decision, approval, attempt, and verdict as insert-only records plus an append-only incident timeline served as ordered CloudEvents with SSE resume.
- FR-13: Ship a disposable minimal Python e-commerce demo (browse, checkout) with three fault injectors — config regression, code bug, inconclusive evidence — whose checkout flow emits a Prometheus error-rate alert delivered by Alertmanager webhook.
- FR-14: Run investigations as a real Temporal `IncidentWorkflow` (`incident-<id>`) on the agent-worker: investigation/sandbox/trusted task queues, 8 min workflow deadline, 25 s activity timeout, approval signal, retries owned only by Temporal activities; the embedded relay wakes on `LISTEN/NOTIFY` (2 s `SKIP LOCKED` fallback) and starts the workflow instead of only recording its ID.
- FR-15: Persist the canonical PDF data model (services/environments, evidence, hypotheses, diagnoses, actions, artifacts, verifications) as insert-only records with incident and action state machines (`RECEIVED→INVESTIGATING→DIAGNOSED→AWAITING_APPROVAL→REMEDIATING→VERIFYING→RESOLVED`, `PROPOSED→TESTING→READY_FOR_REVIEW→APPROVED→DISPATCHING→SUCCEEDED/FAILED/UNKNOWN`) enforced by a transition table and optimistic version check; schema changes ship as Alembic migrations, and compressed bytea artifacts are at most 1 MiB with full SHA-256 hashes.
- FR-16: Split the one image into its four entry points (api with embedded relay, agent-worker with zero write credentials, sandbox-runner with no credentials, trusted-worker with sole staging write credentials); the trusted worker reloads the approved action from PostgreSQL by SHA-256 digest and never accepts action content from the API or agent, and the verifier samples Prometheus telemetry and replays the failing request over the full 10 min window.
- FR-17: Route every model call through a model gateway that reserves worst-case budget before calling, enforces 2 concurrent calls per incident and 16 global, refuses calls after the investigation deadline, stores every attempt (provider, model and checkpoint, prompt version, tokens, cost), provides the tiered triage/hypotheses skill separate from diagnosis and patch, and feeds critic notes into a revised diagnosis (at most 2 revisions); evidence adapters collect logs, metrics, traces, deployment history, and git revision scoped to service and window.
- FR-18: Complete the HTTP contract: `GET /v1/incidents/{id}`, `GET /v1/incidents/{id}/events` (CloudEvents SSE with `Last-Event-ID` resume), `GET /v1/actions/{id}`, `POST /v1/actions/{id}/approvals` carrying the expected digest, `POST /v1/incidents/{id}/cancel`, and `/healthz`, `/readyz`, `/metrics`; approvals require an authenticated approver identity checked against the service's approver role (RBAC), not a free-text name.
- FR-19: Deliver the platform as a root Docker Compose stack (four platform processes, PostgreSQL 16, Temporal, Prometheus, Grafana, demo target) and wire Alertmanager webhooks to `POST /v1/incidents` so the demo alert path is live end to end.

## Non-functional requirements

- NFR-1: Intake p95 at most 300 ms; outbox row to workflow start p95 at most 500 ms; alert to workflow start p95 at most 2 s; intake availability 99.5% monthly.
- NFR-2: Investigation deadline 8 min from alert to diagnosis or escalation; incident may stay open up to 24 h awaiting human approval.
- NFR-3: Per-incident model budget at most USD 0.08 and 80,000 tokens, worst-case reserved before each call; 10 concurrent investigations; 2 model calls per incident; 16 global.
- NFR-4: Tiered inference routing — Gemini 2.5 Flash over API as the default diagnosis and patch model; the critic is always a different model family (an OpenRouter model, or local Llama when no key is configured); free-tier OpenRouter models may serve triage; local Ollama remains for dev and CI and as the offline fallback. Each role is selected by an env spec (`provider:model` — gemini, openrouter or ollama; a bare model name means ollama) resolved in one place (`transport_from_env`), and deterministic evidence collection continues and escalates with a pre-processed bundle when providers are down.
- NFR-5: Monthly infrastructure ceiling USD 150 (alert at 80%, stop non-essential work at 100%); platform recovery RPO 15 min, RTO 2 h from single-database backup.
- NFR-6: Safety invariants hold throughout: model proposes only with read-only tools (I1); only the trusted dispatcher writes approved digests (I2); approvals bind digest, target, revision, policy, approver, expiry (I3); verification is independent telemetry (I4); unknown outcomes reconcile first (I5); application content is untrusted input, never instructions (I6).

## Constraints

- One host, Docker Compose; one container image with four entry points: api with embedded relay, agent-worker with zero write credentials, sandbox-runner with no credentials, trusted-worker holding sole staging write credentials.
- Unified PostgreSQL 16 store for state, outbox, native FTS BM25 runbooks, and bounded compressed bytea artifacts (at most 1 MiB); Temporal for durable execution; OpenTelemetry plus Prometheus plus Grafana for platform health.
- Target applications are Python web services on Linux containers; the platform writes only to a disposable staging copy, never real production.
- Mac toolchain per setup checklist: Colima, uv with Python 3.12, Ollama; API keys (Gemini, OpenRouter) in environment only, never in chat or files.

## Non-goals

- No multi-tenancy, per-tenant billing, or cross-organization data sharing.
- No custom web dashboard; CLI plus HTTP API, Grafana for platform health only.
- No Kubernetes, microservices split, self-hosted GPUs, custom model training, or additional languages beyond Python.
- No production writes and no action without human approval in this version.

## Acceptance criteria

- AC-1: Config-regression fault on checkout produces an alert-to-RESOLVED run with cited diagnosis, sandbox-tested patch, digest-bound approval, and 10 min verification on the timeline.
- AC-2: Code-bug fault produces an accepted diagnosis and a tested patch that passes sandbox gates before approval is requested.
- AC-3: Missing-evidence fault ends INCONCLUSIVE then ESCALATED with partial evidence attached and no patch proposed.
- AC-4: A timed-out dispatch is recorded UNKNOWN, reconciled from the target ledger, and never auto-retried.
- AC-5: Fixture runs stay within USD 0.08 and 80,000 tokens per incident with budget-ledger proof.
- AC-6: Measured intake p95 at most 300 ms and outbox-to-workflow p95 at most 500 ms on the demo host.
- AC-7: [operator] The operator completes every item in `setup-checklist.md` and all of its Verify commands pass on the demo Mac.
- AC-8: [operator] `OPENROUTER_API_KEY` is exported in the operator shell, is non-empty, and appears in no repository file.
- AC-9: [operator] In the showcase the operator performs the approver role, so every applied demo action carries an approval record bound to its digest within the 2 h validity window.
- AC-10: With the root Compose stack up, the config-regression demo alert travels Alertmanager → intake → Temporal workflow → cited diagnosis → sandbox → digest-bound approval → trusted dispatch → 10 min verification → RESOLVED entirely through the four deployed processes (no in-process shortcuts), with the full state-machine history on the incident timeline.

## Post-M5 completion (PDF parity)

M0–M5 delivered milestone slices of `ai-sre-presentation-edition.pdf`; a PDF-vs-code comparison on 2026-11-24 found the integrated Combined System Design (PDF pages 92–108) only partially present. FR-14 through FR-19 and AC-10 close that gap and are implemented after M5 as three tasks: M6-SPINE (FR-14, FR-15), M7-TRUST (FR-16, FR-18), and M8-GATEWAY (FR-17, FR-19). Until M6–M8 pass their gates, describe the system as milestone slices of the design, not the full PDF architecture. Recorded deviations that stand: BM25 ranking in Python over the PG-stored corpus instead of a Postgres FTS extension, and sandbox rootlessness realized as non-root user plus `no-new-privileges` on Docker Desktop.

## Risks

- Model provider outage mid-investigation; mitigation: deterministic evidence plus pre-processed bundle completed first, bounded retries, escalation with evidence.
- Docker-on-Mac sandbox parity for `--network none` rootless semantics; mitigation: Colima default with Docker Desktop fallback, verified by an M3 sandbox gate.
- Budget overrun from oversized contexts; mitigation: Drain3 plus AST compression before any model call with hard gateway caps.
- Approval fatigue or self-approval in single-user showcase; mitigation: digest binding and independent telemetry verdict stand regardless of who approves.

## Open questions

- Exact version pins for PostgreSQL 16 image, Temporal, and Drain3 on this Mac pending toolchain installs (owner: implementer, due at M0; Temporal pin due at M6).
