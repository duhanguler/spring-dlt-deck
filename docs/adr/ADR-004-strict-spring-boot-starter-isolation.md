# ADR-004: Strict Spring Boot 4.2 Starter Isolation Boundary

- **Status:** Accepted
- **Date:** 2026-11-24
- **Context:** Embedding `dlt-deck-spring-boot-starter` inside mission-critical host Spring Boot 4.2 microservices with their own JPA/Hibernate 7, Flyway/Liquibase, Jackson 3, Kafka, Security, and WebMVC configs.

## Problem Statement

Many open-source Spring Boot starters silently break host applications by registering global `WebMvcConfigurer` beans, injecting `@Primary` `ObjectMapper` or `KafkaTemplate` beans, sharing `flyway_schema_history`, leaking JPA `@Entity` scans into the host `EntityManagerFactory`, or overriding the host's `SecurityFilterChain`.

## Decision

`dlt-deck-spring-boot-starter` enforces hard isolation boundaries verified by `archunit-junit6` and `HostAppNonInterferenceIT`:

1. **Zero Global `WebMvcConfigurer`:** Exposes all UI and API routes through an isolated WebMvc.fn `RouterFunction` mounted strictly under `dlt.deck.base-path` (`/dlt-deck`). Host Spring Framework 7 API versioning (`ApiVersionConfigurer`), CORS, and converters remain completely untouched.
2. **Private Jackson 3 `JsonMapper` & Qualified Kafka Beans:** Internal Jackson (`dltDeckJsonMapper`), `AdminClient`, `ProducerFactory`, and `KafkaTemplate` beans are qualified with `@Qualifier("dltDeckInternal")` and never marked `@Primary`.
3. **Zero JPA / Zero Host Flyway Touch:** Storage runs purely on Spring `JdbcClient` + `batchUpdate()` and migrates via an advisory-locked `DltDeckSchemaMigrator` tracked in `dlt_deck_schema_history`.
4. **Scoped Fail-Closed Security:** `dltDeckSecurityFilterChain` applies `http.securityMatcher(basePath, basePath + "/**")` with `@Order(HIGHEST_PRECEDENCE + 20)` and `@ConditionalOnBean(HttpSecurity.class)`, leaving host security rules untouched.
5. **Strict `<optional>true</optional>` Dependencies & Full Backoff:** Security, Redis, Actuator, Micrometer, and OpenTelemetry are strictly optional; setting `dlt.deck.enabled=false` backs off 100% of starter beans.
