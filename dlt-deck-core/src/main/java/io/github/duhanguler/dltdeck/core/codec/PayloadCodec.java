package io.github.duhanguler.dltdeck.core.codec;

import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Binary-safe payload inspection & rendering SPI.
 */
public interface PayloadCodec {

    DecodedPayload decode(byte @Nullable [] payload, Map<String, String> headers, boolean hasDeserializationException);

    record DecodedPayload(
            PayloadFormat format,
            boolean patchable,
            @Nullable Integer schemaId,
            int sizeBytes,
            String displayPreview,
            @Nullable String cloudEventType,
            @Nullable String cloudEventId,
            @Nullable String cloudEventSource) {}
}
