# Stage 1: Build with JDK 25
FROM eclipse-temurin:25-jdk AS builder
WORKDIR /workspace
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY dlt-deck-bom dlt-deck-bom
COPY dlt-deck-core dlt-deck-core
COPY dlt-deck-storage-jdbc dlt-deck-storage-jdbc
COPY dlt-deck-storage-redis dlt-deck-storage-redis
COPY dlt-deck-spring-boot-starter dlt-deck-spring-boot-starter
COPY dlt-deck-mcp dlt-deck-mcp
COPY dlt-deck-test-support dlt-deck-test-support
COPY dlt-deck-benchmarks dlt-deck-benchmarks
COPY dlt-deck-demo dlt-deck-demo
RUN chmod +x mvnw && ./mvnw -pl dlt-deck-demo -am package -DskipTests -B

# Stage 2: Extract Spring Boot layered JAR & train Java 25 AOT Cache (JEP 514/515) + Compact Object Headers (JEP 519)
FROM eclipse-temurin:25-jre-alpine AS trainer
WORKDIR /app
COPY --from=builder /workspace/dlt-deck-demo/target/dlt-deck-demo-*.jar /app/demo.jar
RUN java -Djarmode=tools -jar /app/demo.jar extract --destination /app/extracted
RUN java -XX:+UseCompactObjectHeaders \
         -XX:AOTCacheOutput=/app/app.aot \
         -Dspring.context.exit=onRefresh \
         -Dspring.datasource.url="jdbc:h2:mem:aottrain;DB_CLOSE_DELAY=-1" \
         -Dspring.datasource.username=sa \
         -Dspring.datasource.password= \
         -Dspring.jpa.hibernate.ddl-auto=none \
         -Dspring.flyway.enabled=false \
         -Ddlt.deck.ingestion.enabled=false \
         -Ddlt.deck.storage.auto-migrate=false \
         -Dspring.kafka.bootstrap-servers=localhost:19092 \
         -jar /app/extracted/demo.jar || true

# Stage 3: Minimal production runtime with Java 25 AOT Cache + Compact Object Headers
FROM eclipse-temurin:25-jre-alpine AS runtime
WORKDIR /app
RUN addgroup -S dltdeck && adduser -S dltdeck -G dltdeck
COPY --from=trainer /app/extracted /app/extracted
COPY --from=trainer /app/app.aot /app/app.aot
RUN chown -R dltdeck:dltdeck /app
USER dltdeck
EXPOSE 8080
ENTRYPOINT ["sh", "-c", "exec java -XX:+UseCompactObjectHeaders $([ -f /app/app.aot ] && echo -XX:AOTCache=/app/app.aot) -jar /app/extracted/demo.jar"]
