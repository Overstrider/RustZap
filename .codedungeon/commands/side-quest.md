# side-quest

Use for simple single-repo changes that should be split into a few explicit tasks before implementation.

Steps:
- Resolve or write a short plan under `.codedungeon/plans/`.
- Create `.codedungeon/tasks/side-quest/PLAN.md` plus focused `TASK-NNN.md` files.
- Create or switch to `feat/<slug>` and verify with `./.codex/bin/codedungeon git guard --repo .`.
- Execute tasks in order with focused verification.
- Commit, push, and create or reuse a GitHub PR.
- Run `$code-review`; fix requested changes and rerun review as needed.
- Summarize task completion, PR URL, review verdict, verification, changed files, and risks.

Use `$one-shot` when task splitting is unnecessary. Use `$main-quest` for full phase lifecycle work.
