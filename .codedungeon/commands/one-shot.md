# one-shot

Minimal CodeDungeon workflow for a small Codex task that still needs branch, commit, PR, and review.

Use when the request can be handled by one planner pass and one implementation pass. Do not split into task files, do not run the full phase pipeline, and do not call `codedungeon-loop`.

Steps:
- Validate setup and git repo state without checking protected branches yet.
- Write a short plan to `.codedungeon/plans/one-shot/PLAN.md`.
- Create or switch to `feat/<slug>`, then run `./.codex/bin/codedungeon git guard --repo .` before editing.
- Implement directly from the plan with focused verification.
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

Escalate to `$main-quest` when the request needs multi-repo coordination, explicit QA phases, task decomposition, or a final report.
