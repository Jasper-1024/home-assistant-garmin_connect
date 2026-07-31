# Public feedback pull requests

GitHub Issues and Discussions are disabled on this fork. To send public
feedback, create a pull request that includes a small, redacted Markdown file.
An unchanged fork branch cannot produce a pull request, so do not try to open
an empty pull request.

1. Fork `Jasper-1024/home-assistant-garmin_connect` and create a branch.
2. Add `docs/feedback/<topic>.md` on that branch, replacing `<topic>` with a
   short, descriptive slug.
3. Copy the template below into the new file and provide only the information
   needed to reproduce the problem or evaluate the request.
4. Open a [pull request](https://github.com/Jasper-1024/home-assistant-garmin_connect/pulls)
   with `Jasper-1024/home-assistant-garmin_connect` as the base repository and
   complete the pull-request template.

Do not submit passwords, tokens, diagnostics, logs, screenshots, or raw health
data. Redact account names, identifiers, timestamps, locations, and health
measurements before submitting any example.

## Feedback file template

```markdown
# Feedback: short title

## Fork version

<!-- Example: 3.1.0-beta.2 -->

## Feedback

Describe the problem, feature request, expected result, and actual result.

## Minimal reproduction or rationale

1. First step.
2. Second step.

## Privacy check

I removed passwords, tokens, diagnostics, logs, screenshots, and raw health
data.
```
