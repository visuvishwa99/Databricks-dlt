# Changelog

## Unreleased

- Compare SQL serving text by bytes and length so case-only changes, trailing spaces, and NULL transitions publish correctly on case-insensitive databases.
- Preserve malformed event JSON in bronze and route it to quarantine with an explicit reason.
- Avoid SQL Server writes for unchanged serving rows and add a bounded cleanup procedure for old, unaudited staging attempts.
- Add a separate paused maintenance job and clarify release evidence, simulated scenarios, and deletion boundaries.
- Run CI checks for documentation and repository policy changes.

No production release tag is assigned. The full source, Federation, and SQL Server path has not passed a production acceptance run.
