## Summary

Describe the problem solved and architectural invariants maintained.

## Checklist

- [ ] `./mvnw spotless:check` passes
- [ ] `./mvnw clean verify` passes (ErrorProne + NullAway, jqwik, ArchUnit, JaCoCo, Testcontainers ITs)
- [ ] No global `WebMvcConfigurer` or `ThreadLocal` introduced in starter/core
- [ ] Updated docs / ADR if architectural behavior changed
