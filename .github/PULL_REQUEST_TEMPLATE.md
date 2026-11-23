## Outcome

Describe the user-visible result and the target release in `TASKS.md`.

## Boundary and risk

- Owner: portable task contract / concurrency policy / Tokio / Smol
- Main lifecycle, ordering, capacity, or compatibility risk:
- Explicitly out of scope:

## Verification

- [ ] Formatting and both Clippy configurations pass.
- [ ] Facade builds with neither, Tokio, Smol, and both runtime features.
- [ ] Relevant shared conformance and documentation tests pass.
- [ ] Tests are deterministic; timeouts are watchdogs only.
- [ ] Public docs and `CHANGELOG.md` are updated or not applicable.
