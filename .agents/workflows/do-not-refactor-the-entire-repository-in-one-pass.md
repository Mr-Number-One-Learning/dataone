---
description: Do NOT refactor the entire repository in one pass
---

WORKFLOW REQUIREMENTS

Do NOT refactor the entire repository in one pass.

Instead:

Phase 1:
- Analyze the repository.
- Identify architectural issues.
- Produce a detailed implementation plan.
- Wait for approval.

Phase 2:
- Apply only high-priority, low-risk refactors.
- Run existing tests after each logical change.
- Ensure no regressions.

Phase 3:
- Introduce the metadata layer.
- Introduce lineage abstractions.
- Introduce partition strategy improvements.

Phase 4:
- Update documentation and diagrams.
- Generate an architecture report summarizing all changes.

Never make large, repository-wide changes in a single step.