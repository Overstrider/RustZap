# main-quest

Use for complex features or multi-repo work.

Steps:
- Run `./.codex/bin/codedungeon phase init` if no active run exists.
- Execute phases in order: `0`, `1`, `2'`, `3.5`, `4`, `5`, `5.5`, `5.6`, `6`, `7`.
- For each phase, use `./.codex/bin/codedungeon spawn-prompt <phase>` and the matching Codex subagent when useful.
- If Codex rejects a custom `agent_type`, run `codex features enable multi_agent_v2` or restart Codex with `--enable multi_agent_v2`.
- Preserve the `agent_type`, `model`, and `reasoning_effort` emitted by `spawn-prompt <phase>` when spawning subagents.
- Keep all state changes in codedungeon commands.
- Do not skip review or test phases unless the DB records the skip reason.

Provider behavior:
- Codex commands are playbooks, not assumed slash commands.
- Use Codex agents from `.codex/agents`.
- Use skills from `.agents/skills`.
- Model and effort selection lives in codedungeon DB config, not in Codex agent TOML.
