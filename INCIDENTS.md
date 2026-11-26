# Incident log

Record unplanned failures that affect live pipeline runs or their reports. A failed refresh, a stale warehouse, or an incorrect report can each warrant an entry. Use the actual run logs and validation results as evidence.

## Recorded incidents

No operational incidents have been recorded.

## Entry template

Copy this template into **Recorded incidents** when a live issue occurs. Use timestamps with a time zone and link the affected run or relevant evidence when available. Leave the root cause marked as under investigation until the evidence confirms it.

```markdown
### YYYY-MM-DD — Short description

- **Detected** — When and how the issue was detected, and which run or source snapshot was affected.
- **Impact** — Which data or reports were missing, stale, or incorrect, and for how long.
- **Root cause** — What failed and the evidence that explains why.
- **Fix** — What restored service and what changed to reduce the chance of recurrence.
- **Validation** — Which rerun, freshness check, row-count comparison, or report confirmed recovery.
```
