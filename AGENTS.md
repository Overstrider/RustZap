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

Runtime URL/port rule:
- Never run demos, frontends, backends, or public/local URLs on conventional ports such as 3000, 5000, 5173, 8000, 8080, or other common framework defaults.
- Use high, nonstandard ports and verify they are free with `ss` before starting services.
- Report the exact URL and port used to the user.

Local execution rule:
- For local development, do not tell the user to run commands. Run required installs, builds, tests, servers, and smoke checks yourself, then report the result and exact local URL.
- For staging or production, do not deploy, mutate services, run migrations, restart processes, or execute operational commands unless the user explicitly grants permission for that action.

RustZap business model:
- RustZap is an internal M2M library/service boundary, not an autonomous internet-facing application.
- RustZap must trust the application above it. If the caller says the request is for a company, RustZap accepts that context.
- `company_id` is the required tenant boundary for WhatsApp sessions, chats, messages, media, dirty state, callbacks, and events.
- User identity is optional actor/audit metadata only. It must not partition WhatsApp state. The actor can be a human, automation, or AI.
- RustZap should expose minimal, clean, practical M2M contracts for another backend to consume with `company_id` and optional actor context; prefer normalized public fields such as `delivery_state` instead of forcing callers to interpret internal provider states.
- For user/contact identifiers, expose and accept `phone_number` as digits only, preferably including country code. WhatsApp technical identifiers such as `@lid` and raw JIDs are internal/debug aliases and must not be required as the public M2M ID when a phone number is known.
- Do not add end-user authentication, email verification, passwords, JWT, login screens, or per-user authorization to RustZap.
- Example apps such as `whatsapp-web-shared` should simulate the upper application by selecting or injecting company and optional actor context before calling RustZap.

## codedungeon

Codex CLI pipeline available. Editable command playbooks live in `.codedungeon/commands/`.

| Playbook | Use when |
|----------|----------|
| `one-shot` | Small tasks: plan, code, PR, review; no task split. |
| `side-quest` | Simple tasks, single-repo. |
| `main-quest` | Complex features, multi-repo, full phase pipeline. |
| `code-review` | Standalone adversarial review on current branch. |

Agents in `.codex/agents/`, skills in `.agents/skills/`, commands/phases/mutable state in `.codedungeon/`. CLI binary at `.codex/bin/codedungeon`.
