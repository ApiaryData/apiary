# Review Completed Step

## Instructions

Review the implementation of a completed build step.

1. Read the step's requirements from `docs/prompts/07-v1-development-prompts.md`
2. Check every acceptance criterion against the actual code
3. Verify types match the architecture docs
4. Check that StorageBackend trait is used (never raw filesystem)
5. Check error handling: all errors use ApiaryError, no unwrap() in library code
6. Check that tests exist and cover the acceptance criteria
7. Report: which criteria pass, which need work, any design deviations