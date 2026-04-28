---
name: side-quest
description: Run a compact codedungeon workflow for simple single-repo Codex CLI tasks that need light task splitting.
---

# side-quest

Use for small, well-scoped single-repo changes that should be split into a few explicit tasks before implementation.

Steps:
- Resolve or write a short plan under `.codedungeon/plans/`.
- Create `.codedungeon/tasks/side-quest/PLAN.md` plus focused `TASK-NNN.md` files.
- Create or switch to `feat/<slug>` and verify with `./.codex/bin/codedungeon git guard --repo .`.
- Execute tasks in order with focused verification.
- Commit, push, and create or reuse a GitHub PR.
- Run `$code-review`; fix requested changes and rerun review as needed.
- Summarize task completion, PR URL, review verdict, verification, changed files, and risks.

Keep DB phase state only when the task has already been bootstrapped into a codedungeon run.
Use `$one-shot` when task splitting is unnecessary. Use `$main-quest` for full phase lifecycle work.
