package io.github.duhanguler.dltdeck.demo;

import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.spi.DltDryRunHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import org.jspecify.annotations.Nullable;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Host application opt-in Tier-2 {@link DltDryRunHandler} SPI validating payment currency and amount
 * without performing any database writes or external side effects.
 */
@Component
public class PaymentOrderDryRunHandler implements DltDryRunHandler {

    private static final Set<String> SUPPORTED_CURRENCIES = Set.of("TRY", "USD", "EUR");
    private final JsonMapper jsonMapper = JsonMapper.builder().build();

    @Override
    public boolean supports(String targetTopic) {
        return "payments.events".equals(targetTopic);
    }

    @Override
    public HandlerResult dryRun(String targetTopic, DltMessageDetail message, byte @Nullable [] candidatePayload) {
        if (candidatePayload == null || candidatePayload.length == 0) {
            return new HandlerResult(false, "Payload is empty", List.of("Payments require a JSON body"));
        }
        try {
            JsonNode node = jsonMapper.readTree(new String(candidatePayload, StandardCharsets.UTF_8));
            String currency = node.has("currency") ? node.get("currency").asString() : "";
            long amount = node.has("amountCents") ? node.get("amountCents").asLong() : -1L;
            if (!SUPPORTED_CURRENCIES.contains(currency)) {
                return new HandlerResult(
                        false,
                        "Unsupported currency '" + currency + "'",
                        List.of("Allowed ISO currencies: " + SUPPORTED_CURRENCIES));
            }
            if (amount <= 0) {
                return new HandlerResult(
                        false,
                        "amountCents must be positive",
                        List.of("Observed amountCents=" + amount));
            }
            return new HandlerResult(
                    true,
                    "Payment domain invariants verified (currency=" + currency + ", amountCents=" + amount + ")",
                    List.of("Ready for four-eyes replay to " + targetTopic));
        } catch (JacksonException ex) {
            return new HandlerResult(false, "Malformed JSON payload", List.of(ex.getOriginalMessage()));
        }
    }
}
