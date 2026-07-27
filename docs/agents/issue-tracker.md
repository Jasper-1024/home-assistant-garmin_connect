# Issue tracker: Plane

Specs, tickets, requests, and implementation status for this repository live in
Plane.

## Project

- Workspace: `work`
- Project: `GARMIN` — Home Assistant Garmin Connect
- Project ID: `d615a7c3-b0b3-48cd-94a8-20a85d875bb3`
- GitHub is used only for source control. Do not use GitHub Issues for this
  repository's engineering workflow.

## Tool

Use the `plane-ops` skill and its bundled `scripts/plane-ops.sh` CLI for all
tracker operations. Do not use Plane MCP.

Pass this project to every project-scoped command:

```bash
scripts/plane-ops.sh \
  --project-id d615a7c3-b0b3-48cd-94a8-20a85d875bb3 \
  <command>
```

Credentials come from the OS keyring. Never write or print Plane API keys in
repository files, logs, comments, or dry-run output.

## Conventions

- Create a work item:
  `work-item create --name "..." --description "..." --state Todo`
- Read a work item: `work-item get GARMIN-<number>`
- List active work: `work-item list`
- Comment: `work-item comment GARMIN-<number> --body "..."`
- Update state or labels:
  `work-item update GARMIN-<number> --state "..." --label "..."`
- Complete: `work-item move GARMIN-<number> --state Done`
- Archive only when explicitly requested:
  `work-item archive GARMIN-<number>`

Use readable identifiers such as `GARMIN-12` in prose. Use UUIDs for bulk
scripting when already known.

## Specs and tickets

A published spec is a Plane work item containing the complete accepted
specification.

Implementation tickets are child work items of that spec. Tickets produced by
`/to-tickets` are already agent-ready; apply `ready-for-agent` directly and do
not run `/triage` on them.

Each ticket must include:

- acceptance criteria
- relevant spec and research references
- scope exclusions
- verification requirements
- explicit blocking edges

## Blocking relations

Use Plane native relations as the canonical dependency representation:

```bash
scripts/plane-ops.sh \
  --project-id d615a7c3-b0b3-48cd-94a8-20a85d875bb3 \
  work-item relations create GARMIN-<ticket> GARMIN-<blocker> \
  --relation-type blocked_by
```

A ticket is ready only when every `blocked_by` relation points to a completed
item.

## When a skill says "publish to the issue tracker"

Create a Plane work item in project `GARMIN`.

## When a skill says "fetch the relevant ticket"

Run `work-item get GARMIN-<number>`, then inspect comments and relations when
relevant.

## Request triage

Incoming reports and requests start in `Backlog` with `needs-triage`.

Generated implementation tickets start in `Todo` with `ready-for-agent`.

Use the label mapping in `docs/agents/triage-labels.md`.
