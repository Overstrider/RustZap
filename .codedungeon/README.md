# .codedungeon

Project-local CodeDungeon runtime state.

- codedungeon.db: SQLite source of truth for runs, phases, tasks, handoffs, findings, and model configuration.
- commands/: editable workflow playbooks; Claude keeps thin slash-command wrappers in .claude/commands.
- phases/: editable phase prompts installed by CodeDungeon.
- plan/ and state/: human-readable views rendered from the DB.
- tasks/: task plans and execution notes.
- reviews/: adversarial review inputs and outputs.
- memory/prs/: durable PR/run summaries for later investigation.
- memory/runs/: durable run reports for continuation and audit.
- archive/: migrated or conflicting legacy runtime state.

Provider-native directories such as .claude, .codex, and .agents keep only files required by those CLIs.
