---
name: code-review
description: Run a standalone codedungeon-style adversarial review in Codex CLI.
---

# code-review

Use for reviewing the current branch or an implementation diff.

Review order:
- Correctness regressions.
- Security and data handling.
- Missing or weak tests.
- Maintainability only when it creates concrete risk.

Output findings first, ordered by severity. Include file and line references. If there are no actionable findings, say so directly.
