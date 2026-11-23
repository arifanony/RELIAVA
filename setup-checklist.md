# AI SRE — Mac Setup Checklist (pre-Genesis)

Goal: make this Mac ready for Genesis `new-product` with objective AI SRE.
Frozen contract: single tenant, Python-only, one-host Docker Compose, FastAPI + Temporal + PostgreSQL 16, $0.08 / 80k tokens per incident, invariants I1–I6.
No product code and no `genesis init` until every box below is checked.

## 0. Prerequisite
- [ ] macOS with Xcode Command Line Tools (`git --version` already passes on this Mac).

## 1. Homebrew (missing on this Mac)
- [ ] Install from https://brew.sh, then verify:
  `brew --version`

## 2. Docker provider (missing; Colima is the open-source default)
- [ ] Install and start: `brew install colima docker docker-compose && colima start`
- [ ] Verify: `docker --version && docker compose version && docker ps`
- [ ] Fallback only if Colima sandbox differs: Docker Desktop.
- [ ] Later M3 gate: a `--network none` test container runs with no credentials.

## 3. Python toolchain (system is 3.9.6 — too old)
- [ ] Install uv: `brew install uv`
- [ ] Pin Python 3.12: `uv python install 3.12 && uv python pin 3.12`
- [ ] Verify: `uv --version && uv run python --version`

## 4. Ollama for free dev/CI (missing)
- [ ] Install: `brew install ollama`
- [ ] Pull dev model per PDF: `ollama pull qwen2.5-coder:7b`
- [ ] Verify: `ollama --version && ollama list`

## 5. OpenRouter (key held by user — never paste it in chat or files)
- [ ] Export per shell: `export OPENROUTER_API_KEY="$OPENROUTER_API_KEY"`
- [ ] Models for M1 research: Gemini 2.5 Flash / DeepSeek-V3 (triage), Qwen-2.5-Coder-32B (diagnosis/patch), Claude 3.5 Haiku (critic).
- [ ] Verify without printing the key: `test -n "$OPENROUTER_API_KEY" && echo ok`.

## 6. Genesis readiness
- [ ] `node --version` passes (v22 already on this Mac; needs 18+).
- [ ] `node sources/genesis-kit/tools/genesis.mjs --help` runs (already verified).
- [ ] M0 version pins to confirm during spec: PostgreSQL 16, Temporal, Drain3, Python AST (stdlib).
- [ ] Target demo: minimal Python e-commerce with a config-regression fault.

## Done means
All `Verify` commands pass. Then we start Genesis with objective AI SRE.
