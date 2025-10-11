# Contributing Guidelines (Reuse First)

- Prefer reuse of existing components in this repo or pinned external repos.
- Do not silently fork external scripts; if behavior must change, document why and vendor with attribution.
- Update `docs/COMPONENTS.md` and `_manifests/versions.json` when adding/upgrading external dependencies.
- Preserve interface contracts in `docs/INTERFACES.md`; if you must change a contract, update the doc and note the rationale.
- Use atomic writes and idempotent patterns; never require a clean slate to re-run.
- Avoid symlinks in critical paths; prefer regular files or hard links (same filesystem).
- For new tools:
  - Add a short usage example, inputs/outputs, and failure behavior.
  - Adhere to existing directory layouts and manifest conventions.
