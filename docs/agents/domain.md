# Domain Docs

How engineering skills consume this repository's domain documentation.

## Before exploring, read these

- `CONTEXT.md` at the repository root.
- Relevant ADRs under `docs/adr/`.

If these files do not exist, proceed silently. Do not create them merely because
they are absent. `/domain-modeling`, `/grill-with-docs`, or an architecture
workflow creates them when terminology or decisions need recording.

## Layout

This is a single-context repository:

```text
/
├── CONTEXT.md
├── docs/
│   └── adr/
└── custom_components/
    └── garmin_connect/
```

## Use the glossary vocabulary

When output names a domain concept—in a spec, ticket title, proposal,
hypothesis, or test—use the term defined in `CONTEXT.md`.

If a required concept is missing, reconsider whether new terminology is
necessary or record the gap for `/domain-modeling`.

## Flag ADR conflicts

If proposed work contradicts an existing ADR, surface the conflict explicitly
instead of silently overriding it:

> Contradicts ADR-0007 — worth reopening because…
