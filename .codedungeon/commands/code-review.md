# code-review

Use for standalone adversarial review of the current branch.

Review order:
- Correctness and regressions.
- Security and data handling.
- Tests and missing coverage.
- Maintainability only when it creates concrete risk.

Output:
- Findings first, ordered by severity.
- Include file and line references.
- Include no finding if no actionable issue exists.
