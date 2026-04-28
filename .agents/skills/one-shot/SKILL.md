---
name: one-shot
description: "Run a minimal CodeDungeon workflow for one small Codex task: plan, implement, create PR, and run review without task splitting."
---

# one-shot

Use for small, well-scoped changes that still need branch, commit, PR, and adversarial review.

Workflow:
- Validate setup and git repo state without checking protected branches yet.
- Write a short plan to `.codedungeon/plans/one-shot/PLAN.md`.
- Create or reuse a `feat/<slug>` branch, then run `./.codex/bin/codedungeon git guard --repo .` before editing.
- Implement directly from the plan; do not create `.codedungeon/tasks/*`.
- Run focused verification.
- Commit, push, and reuse the current branch PR when it exists; otherwise create one.
- Run `$code-review` against the PR.
- If review requests changes, fix directly and rerun review up to 3 cycles.

Return:
- `ONE_SHOT_COMPLETE`
- branch
- PR URL
- review verdict
- verification commands and results
- changed files and residual risks

Use `$side-quest` when there is already a plan that should be split into tasks. Use `$main-quest` for full phase lifecycle work.
