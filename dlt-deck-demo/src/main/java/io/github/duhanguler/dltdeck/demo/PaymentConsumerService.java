package io.github.duhanguler.dltdeck.demo;

import io.github.duhanguler.dltdeck.starter.kafka.DltDeckIdempotentReplayFilter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.resilience.annotation.ConcurrencyLimit;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Host application payment event consumer demonstrating seamless integration with
 * {@link DltDeckIdempotentReplayFilter} and Spring Framework 7 {@link ConcurrencyLimit}.
 */
@Service
public class PaymentConsumerService {

    private static final Logger log = LoggerFactory.getLogger(PaymentConsumerService.class);
    private static final Set<String> VALID_CURRENCIES = Set.of("TRY", "USD", "EUR");

    private final PaymentOrderRepository repository;
    private final DltDeckIdempotentReplayFilter idempotentReplayFilter;
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    public PaymentConsumerService(
            PaymentOrderRepository repository, DltDeckIdempotentReplayFilter idempotentReplayFilter) {
        this.repository = repository;
        this.idempotentReplayFilter = idempotentReplayFilter;
    }

    @KafkaListener(topics = "payments.events", groupId = "payments-demo-consumer", autoStartup = "${dlt.deck.ingestion.enabled:true}")
    @ConcurrencyLimit(8)
    public void onPaymentEvent(ConsumerRecord<String, String> record) {
        if (!idempotentReplayFilter.shouldProcess(record)) {
            log.info("Deduplicated replayed payment event via x-dlt-deck-idempotency-key");
            return;
        }
        String body = record.value();
        if (body == null) {
            return;
        }
        JsonNode node = jsonMapper.readTree(body.getBytes(StandardCharsets.UTF_8));
        String orderId = node.has("orderId") ? node.get("orderId").asString() : "ORD-UNKNOWN";
        String customer = node.has("customerName") ? node.get("customerName").asString() : "Demo Customer";
        String currency = node.has("currency") ? node.get("currency").asString() : "TRL";
        long amountCents = node.has("amountCents") ? node.get("amountCents").asLong() : 1000L;
        String stage = node.has("stage") ? node.get("stage").asString() : "CAPTURED";

        if (!VALID_CURRENCIES.contains(currency)) {
            throw new IllegalArgumentException("Unsupported currency: " + currency);
        }
        repository.save(new PaymentOrderEntity(orderId, customer, currency, amountCents, stage, Instant.now()));
        log.info("Processed payment order={} stage={} currency={} amountCents={}", orderId, stage, currency, amountCents);
    }
}
