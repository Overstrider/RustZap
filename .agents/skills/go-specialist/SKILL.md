---
name: go-specialist
description: Go implementation and review guidance for codedungeon Codex flows.
---

# Go Specialist

- Prefer small interfaces at call sites.
- Keep errors contextual.
- Use table tests for behavior matrices.
- Avoid global provider state in tests unless reset helpers exist.
- Run `go test ./...` when blast radius is broad.
