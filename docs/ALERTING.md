# Alerting Rules

Define alert thresholds for critical metrics to detect service degradation quickly.

Example Prometheus alert rules:

```
- alert: HighErrorRate
  expr: claims_failed / (claims_processed + 1) > 0.05
  for: 5m
  labels:
    severity: critical
  annotations:
    summary: High claim failure rate detected
```

Configure alerts to notify on-call engineers via email or messaging channels when thresholds are exceeded.
