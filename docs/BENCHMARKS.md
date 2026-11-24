# Benchmarks & Verification Metrics

All benchmarks below were measured directly on **OpenJDK 25.0.5.1 (Ubuntu 24.04.2 LTS, x86_64, 10 vCPUs)** using **JMH 1.37** (`-wi 2 -i 3 -f 1`, `Mode.AverageTime`, `TimeUnit.MICROSECONDS`) with zero synthetic projections.

## 1. Core Engine JMH Microbenchmarks — Standard vs. Java 25 Compact Object Headers (`JEP 519`)

```bash
./mvnw -pl dlt-deck-benchmarks -am package -DskipTests
java -jar dlt-deck-benchmarks/target/benchmarks.jar
java -XX:+UseCompactObjectHeaders -jar dlt-deck-benchmarks/target/benchmarks.jar
```

| Benchmark | What It Measures | Standard Headers (µs/op) | `-XX:+UseCompactObjectHeaders` (µs/op) | Approx. Throughput |
| :--- | :--- | :---: | :---: | :---: |
| `sniffConfluentBinaryPayload` | Confluent `0x00` magic byte + 4-byte schemaId + `NOT_PATCHABLE` guard | `0.186` | **`0.178`** (-4.3%) | **~5.6M ops/s** |
| `sniffJsonPayload` | Strict UTF-8 decoding + Jackson 3 structural JSON validation | `1.720` | `1.788` | **~560k ops/s** |
| `fingerprintException` | SHA-256 + UUID/ISO-TS/hex/numeric/CGLIB stack normalization + category | `18.320` | `18.596` | **~54k ops/s** |
| `redactChecksumVerifiedPii` | TCKN (mod-10/11) + IBAN (ISO 13616 mod-97) + Luhn PAN + JWT + email + phone + secrets | `51.784` | **`49.991`** (-3.5%) | **~20k ops/s** |
| `computeMerkleRoot50Records` | 50-leaf SHA-256 domain-separated binary Merkle tree root | `70.602` | **`65.277`** (-7.5%) | **~15.3k batches/s** (~765k msgs/s) |

## 2. Static Guarantees & Test Coverage (`dlt-deck-core`)

| Quality Gate | Tooling | Measured Result |
| :--- | :--- | :--- |
| **Compile-time Null Safety** | ErrorProne `2.53.0` + NullAway `0.14.4` (`JSpecifyMode=true`, `@NullMarked`) | **0 errors, 0 warnings** |
| **Line Coverage (`dlt-deck-core`)** | JaCoCo `0.8.16` (enforced minimum: `85%`) | **97.63%** (`990 / 1014` lines) |
| **Branch Coverage (`dlt-deck-core`)** | JaCoCo `0.8.16` (enforced minimum: `80%`) | **93.85%** (`565 / 602` branches) |
| **Property-Based Fuzzing** | `jqwik 1.10.2` (100–150 randomized + edge-case tries per invariant) | **8/8 properties passing** |
| **Architectural Invariants** | `archunit-junit6 1.5.1` (core purity, zero `ThreadLocal`, zero starter `WebMvcConfigurer`, fail-closed MCP, JEP 290 `ObjectInputStream` isolation) | **100% passing** |
| **Live Fault-Injection ITs** | Testcontainers `2.0.5` (Apache Kafka 4 KRaft + PostgreSQL 17 + Redis 7) | **8/8 IT classes passing** |
