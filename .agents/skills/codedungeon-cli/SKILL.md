---
name: codedungeon-cli
description: Use codedungeon CLI commands safely inside Codex CLI workflows.
---

# codedungeon CLI

Use when running or composing codedungeon commands.

Rules:
- Resolve the project root before DB-touching commands.
- Use the project-local binary: `./.codex/bin/codedungeon`.
- Prefer `./.codex/bin/codedungeon phase info` before changing phase state.
- Read model and effort with `./.codex/bin/codedungeon config model <tier>` and `./.codex/bin/codedungeon config effort <tier>`.
- Use provider paths from `./.codex/bin/codedungeon status` and installed artifact metadata.
- Do not assume Codex command playbooks are slash commands.
