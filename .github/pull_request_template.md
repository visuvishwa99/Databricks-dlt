## Change and reason

Describe the source contract, pipeline, SQL serving, or operational behavior changed.

## Verification

- [ ] `ruff check .`
- [ ] Unit tests and SQL integration tests where relevant
- [ ] Bundle validation for affected targets
- [ ] Workspace run or drill ID recorded for runtime changes

## Data and release impact

- [ ] Document schema, replay, checkpoint, serving, and rollback implications
- [ ] Update `docs/evidence.md` with observed results; label unrun checks as pending
- [ ] Confirm schedules remain paused unless this PR explicitly authorizes activation
