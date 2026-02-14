# Implement Build Step

## Instructions

I want to implement a build step from the Apiary development prompts.

1. Read `docs/BUILD_STATUS.md` to confirm which steps are already complete
2. Read the target step from `docs/prompts/07-v1-development-prompts.md`
3. Read the referenced architecture documents for that step
4. Implement the step following the requirements exactly
5. Run `cargo clippy --workspace` and `cargo test --workspace` after
6. Update `docs/BUILD_STATUS.md` to mark the step complete

## Rules

- Read the architecture docs BEFORE writing code
- Follow the acceptance criteria exactly — each one must pass
- Create types and APIs as specified in the prompt
- Write unit tests for every public function
- Use `tracing` for debug logging at decision points
- If a step depends on a previous step's output, verify it exists first