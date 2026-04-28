---
name: cleanup-tasks
description: Reconcile stale codedungeon task artifacts and state in Codex CLI.
---

# cleanup-tasks

Use when task state or temporary artifacts need cleanup.

Steps:
- Inspect codedungeon task and phase state first.
- Verify each task against the working tree.
- Mark tasks done, blocked, or obsolete with evidence.
- Delete only ephemeral provider artifact contents when explicitly requested.
- Preserve DB history and installed provider pack files.
