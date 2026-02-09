# Generate Tests for Build Step

## Instructions

Generate comprehensive tests for a completed build step.

1. Read the step from `docs/prompts/07-v1-development-prompts.md`
2. Read the implementation
3. Create unit tests in `#[cfg(test)]` modules
4. Create integration tests in `tests/integration/`
5. Cover all acceptance criteria
6. Include edge cases: empty data, concurrent access, error paths
7. Use a temporary directory for LocalBackend tests (clean up after)