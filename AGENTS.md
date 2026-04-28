# codedungeon for Codex CLI

Use codedungeon as the deterministic workflow kernel. Preserve the phase flow, DB state, handoff schema, review JSON, and task contracts.

Project artifacts:
- Workflow skills: `.agents/skills/main-quest/`, `.agents/skills/side-quest/`, `.agents/skills/one-shot/`, `.agents/skills/code-review/`
- Editable command playbooks for reference: `.codedungeon/commands/`
- Phase instructions: `.codedungeon/phases/`
- Codex subagents: `.codex/agents/`
- Codex skills: `.agents/skills/`
- Local binary and DB: `./.codex/bin/codedungeon`, `.codedungeon/codedungeon.db`

Default workflow:
- Invoke workflows as skills: `$main-quest`, `$side-quest`, `$one-shot`, `$code-review`, `$codedungeon-test-loop`, `$cleanup-tasks`.
- If Codex rejects a custom `agent_type`, run `codex features enable multi_agent_v2` or restart Codex with `--enable multi_agent_v2`.
- Use `./.codex/bin/codedungeon phase info` before changing phase state.
- Use `./.codex/bin/codedungeon spawn-prompt <phase>` to compose runtime phase context.
- Preserve the `agent_type`, `model`, and `reasoning_effort` emitted by `spawn-prompt <phase>` when using Codex subagents.
- Close completed phases with `./.codex/bin/codedungeon phase done`.
- Treat `.codedungeon/commands/` as reference playbooks, not Codex CLI slash commands.
- Keep provider-specific instructions in Codex files; do not copy Claude-only syntax into Codex prompts.

## codedungeon

Codex CLI pipeline available. Editable command playbooks live in `.codedungeon/commands/`.

| Playbook | Use when |
|----------|----------|
| `one-shot` | Small tasks: plan, code, PR, review; no task split. |
| `side-quest` | Simple tasks, single-repo. |
| `main-quest` | Complex features, multi-repo, full phase pipeline. |
| `code-review` | Standalone adversarial review on current branch. |

Agents in `.codex/agents/`, skills in `.agents/skills/`, commands/phases/mutable state in `.codedungeon/`. CLI binary at `.codex/bin/codedungeon`.

