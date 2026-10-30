# Instructions for AI Agents

## Opening Pull Requests

### Use the PR Template

All pull requests **must** use the PR template located at `.github/pull_request_template.md`.
Fill in **every section** of the template meaningfully — do not leave placeholder text such as
`<!-- describe the motivation -->` or `…` in the final PR description.

### Sections to Complete

| Section | What to write |
|---|---|
| **Summary – Why** | Explain the motivation or problem being solved. |
| **Summary – What** | Describe the changes introduced by this PR. |
| **Expected Behavior** | Describe what should happen after the change. |
| **Actual Behavior** | Describe what was happening before (for bug fixes) or the new capability (for features). |
| **Steps to Reproduce** | List concrete reproduction steps (for bug fixes). |
| **Known Workarounds** | Note any workarounds if applicable; otherwise remove the section or write "None". |
| **Additional Evidence** | Attach relevant logs, screenshots, or environment details. |
| **Categorization** | Check all applicable boxes. |

### Link to an Existing Issue

Every PR **must** be linked to an existing issue(if there's pre-existing one). Replace the issue placeholder
template:

```
This PR resolves #<Replace-Me-With-The-Issue-Number-Addressed-By-This-PR> if any.
```

with the actual issue number, for example:

```
This PR resolves #42.
```

If no issue exists, please remove the issue placeholder.

## Contribution Guidelines Summary

The full guidelines are in [CONTRIBUTING.md](./CONTRIBUTING.md). Key points for PRs:

1. **All new features must have passing tests.** A submitted PR should have already been tested
   for both existing and new unit tests.
2. **Bug fixes must include a test case** that demonstrates the error being fixed.
3. **Open an issue first** before submitting a PR for large features. Large features that have
   never been discussed are unlikely to be accepted.
4. **Do not create WIP PRs.** Do not submit pull requests with "work-in-progress" changes.
5. **Use clear and concise PR titles.** Titles should accurately reflect the scope of the change.
6. **Each PR must be linked to an existing issue.** If no issue exists, create one first.

## Writing the PR Description

- Write in clear English.
- Include enough context so that reviewers unfamiliar with the change can understand it.
- Reference related issues, prior discussions, or relevant documentation where helpful.
- Do not copy-paste large blocks of code into the description; use `<details>` tags for lengthy
  log output or stack traces.

## Maintaining the OpenAPI Contract

The API specification is hand-maintained in `cruise-control/src/main/resources/yaml/`.
The generator bundles this YAML; it does not infer the contract from Java.
When changing request parsing, response serialization, HTTP status codes, or headers:

1. Update the YAML and its contract regressions in `RequestParameterTest` and `ResponseTest`.
   Keep header parameters separate from query-parameter allowlists. Check wire serialization
   against `ParameterUtils` and task headers against `UserTaskManager`.
2. Run `./gradlew :cruise-control:test --tests '*RequestParameterTest' --tests '*ResponseTest'` and
   if failures occur, fix them.
3. Regenerate with `./gradlew :cruise-control:generateOpenApiJson --rerun-tasks`.
   The output is `cruise-control/src/main/resources/webroot/openapi.json`. Do not edit it by hand.
4. Review the generated diff.