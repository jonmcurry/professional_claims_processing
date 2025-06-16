# Change Management Procedures

This document outlines the procedures for introducing changes to the claims processing system.

## Proposal
- Create a short design proposal for significant changes.
- Document potential impacts and rollback steps.

## Review and Approval
- Submit code via pull request and request at least one reviewer.
- Ensure tests and security scans pass before merging.

## Deployment
- Use the deployment checklist in [docs/DEPLOYMENT.md](DEPLOYMENT.md).
- Record configuration or schema modifications in `migrations/README.md`.

## Rollback
- Tag releases so that previous versions can be redeployed quickly.
- Maintain database backups for point‑in‑time recovery.
