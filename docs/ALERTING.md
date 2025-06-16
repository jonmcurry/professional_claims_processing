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

## Escalation Policies

1. Notifications are sent to the recipients listed in `config.yaml`.
2. If an alert persists for more than 30 minutes, escalate to the engineering
   manager and open a high priority incident.
3. Critical alerts, such as sustained CPU usage above 90%, require an immediate
   phone call to the on-call engineer.
