---
name: main-quest
description: Run the full codedungeon phase workflow for complex or multi-repo Codex CLI implementation work.
---

# main-quest

Use for complex features, multi-repo changes, or work that needs the full phase lifecycle.

Steps:
- Ensure a codedungeon run exists with `./.codex/bin/codedungeon phase init` when needed.
- Execute phases in order: `0`, `1`, `2'`, `3.5`, `4`, `5`, `5.5`, `5.6`, `6`, `7`.
- For each phase, inspect state with `./.codex/bin/codedungeon phase info <phase>`.
- Use `./.codex/bin/codedungeon spawn-prompt <phase>` to compose phase context.
- If Codex rejects a custom `agent_type`, run `codex features enable multi_agent_v2` or restart Codex with `--enable multi_agent_v2`.
- When spawning a Codex subagent, pass the `agent_type`, `model`, and `reasoning_effort` emitted by `spawn-prompt <phase>`.
- Use Codex subagents from `.codex/agents` only when delegation is explicitly useful; do not rely on agent TOML files to choose model or effort.
- Close each phase with `./.codex/bin/codedungeon phase done`.

Do not treat `.codedungeon/commands` as an executable command system. Those files are editable reference playbooks.
