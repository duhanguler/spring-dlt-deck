package io.github.duhanguler.dltdeck.core.model;

import java.util.Arrays;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Full message detail combining hot metadata, deduplicated fingerprint info, and cold payload/headers.
 */
@SuppressWarnings("ArrayRecordComponent")
public record DltMessageDetail(
        DltMessageSummary summary,
        FingerprintCluster cluster,
        byte @Nullable [] messageKey,
        byte @Nullable [] payload,
        String payloadEncoding,
        @Nullable String externalBlobUri,
        Map<String, String> headers,
        @Nullable String cloudEventType,
        @Nullable String cloudEventId,
        @Nullable String cloudEventSource) {

    public DltMessageDetail(
            DltMessageSummary summary,
            FingerprintCluster cluster,
            byte @Nullable [] messageKey,
            byte @Nullable [] payload,
            String payloadEncoding,
            @Nullable String externalBlobUri,
            Map<String, String> headers,
            @Nullable String cloudEventType,
            @Nullable String cloudEventId,
            @Nullable String cloudEventSource) {
        this.summary = summary;
        this.cluster = cluster;
        this.messageKey = messageKey == null ? null : Arrays.copyOf(messageKey, messageKey.length);
        this.payload = payload == null ? null : Arrays.copyOf(payload, payload.length);
        this.payloadEncoding = payloadEncoding;
        this.externalBlobUri = externalBlobUri;
        this.headers = Map.copyOf(headers);
        this.cloudEventType = cloudEventType;
        this.cloudEventId = cloudEventId;
        this.cloudEventSource = cloudEventSource;
    }

    @Override
    public byte @Nullable [] messageKey() {
        return messageKey == null ? null : Arrays.copyOf(messageKey, messageKey.length);
    }

    @Override
    public byte @Nullable [] payload() {
        return payload == null ? null : Arrays.copyOf(payload, payload.length);
    }
}
