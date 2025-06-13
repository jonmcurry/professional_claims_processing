# Security Scanning Integration

Static code analysis helps detect common vulnerabilities early in development. A simple `bandit` configuration can be used:

```bash
pip install bandit
bandit -r src -ll
```

Running the command as part of CI or a pre‑commit hook ensures security issues are caught before deployment.
