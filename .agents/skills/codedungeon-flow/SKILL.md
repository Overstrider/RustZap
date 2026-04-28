---
name: codedungeon-flow
description: Preserve codedungeon's shared phase lifecycle across providers.
---

# codedungeon Flow

The provider can change prompts, agents, skills, commands, and install paths. The shared flow does not change:
- phase IDs and order
- handoff schema
- task state
- review finding schema
- verification-before-completion
- final report contract

Use provider-native prompt surfaces only at the execution edge.
