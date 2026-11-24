# Open-Source Launch & Community Distribution Checklist

## 1. GitHub Repository Metadata

- **Description:** `Zero-interference Spring Boot 4.2 + Java 25 Kafka Dead-Letter Topic (DLT) Triage, Poison-Pill Recovery, Honest 3-Tier Dry-Run, and Key-Ordered Fail-Closed Replay Control Plane`
- **Topics:** `spring-boot`, `spring-kafka`, `apache-kafka`, `dead-letter-queue`, `kafka-dlt`, `java-25`, `distributed-systems`, `spring-boot-starter`, `observability`, `mcp`, `postgresql`, `fintech`
- **Social Preview Image:** Upload `docs/assets/workbench-dark.png` (or a 1280x640 banner) under **Settings -> Social preview**.

## 2. High-Signal Community Launch Posts

### Show HN / Hacker News Title
> `Show HN: spring-dlt-deck – Fail-closed Kafka DLT triage & key-ordered replay for Spring Boot 4.2 + Java 25`

Key talking points that resonate with Staff/Principal engineers:
- Honest crash recovery: why holding `FOR UPDATE SKIP LOCKED` across Kafka I/O starves HikariCP, why `enable.idempotence=true` does **not** give cross-restart EOS, and how Two Short TXs + Fencing Tokens + `INDETERMINATE` solves it honestly (`ADR-001`).
- Multi-pod key-ordered replay via deterministic stripe leasing + Per-Key Head-of-Line Barrier (`ADR-002`).
- Zero host-app side effects: zero global `WebMvcConfigurer`, private Jackson 3 `JsonMapper`, isolated `dlt_deck_schema_history`, JEP 290/415 `ObjectInputFilter`, and ArchUnit-verified fail-closed MCP (`ADR-004`, `ADR-005`).

### Reddit (`r/java`, `r/SpringBoot`, `r/apachekafka`)
- Share the **60-second Docker Compose quickstart** + Dark/Light Workbench screenshots + real JMH Java 25 `-XX:+UseCompactObjectHeaders` numbers + link directly to `docs/adr/`.

### *This Week in Spring* / *Java Weekly*
- Tag Josh Long (`@starbuxman`) / Baeldung Java Weekly with the release link, noting Spring Boot 4.2, Spring Framework 7 `@ConcurrencyLimit` + Native API Versioning, Apache Kafka 4.4 KIP-848, and Java 25 LTS (`ScopedValue`, `Stream.gather`, JEP 513, AOT Cache, Compact Object Headers).
