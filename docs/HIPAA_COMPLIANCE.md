# HIPAA Compliance Checklist

This project processes Protected Health Information (PHI). The following checklist helps ensure HIPAA compliance throughout development and operations.

## Administrative Safeguards
- Conduct regular risk assessments and document findings.
- Maintain workforce training records for handling PHI.
- Designate a privacy and security officer responsible for compliance.

## Technical Safeguards
- Encrypt PHI at rest and in transit using the `encryption_key` from `config.yaml`.
- The application requires the `cryptography` package to be installed for these
  encryption features.
- Ensure user authentication for all web endpoints via API key and role headers.
- Record access events in the `audit_log` table for traceability.

## Physical Safeguards
- Restrict server access to authorized personnel.
- Store backups in secure locations with limited physical access.

## Policies and Procedures
- Review data retention policies annually and update as needed.
- Document breach notification procedures and test them periodically.

This checklist is not exhaustive but provides a baseline for integrating HIPAA requirements into the claims processing workflow.
