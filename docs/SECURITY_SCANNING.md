# Security Scanning Integration

Static code analysis helps detect common vulnerabilities early in development. A simple `bandit` configuration can be used:

```bash
pip install bandit
bandit -r src -ll
```

Running the command as part of CI or a pre‑commit hook ensures security issues are caught before deployment.

Our GitHub Actions workflow installs `bandit` and scans the `src` directory on every pull request.
Failures will block the merge until issues are resolved.
