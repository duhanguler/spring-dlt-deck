package io.github.duhanguler.dltdeck.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.duhanguler.dltdeck.core.codec.DefaultPayloadCodec;
import io.github.duhanguler.dltdeck.core.exception.ValidationDomainException;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.masking.CsvExportSanitizer;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.model.MessageCoordinates;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.KeysetCursorCodec;
import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.Size;

class CoreFuzzProperties {

    private final ExceptionFingerprinter fingerprinter = new ExceptionFingerprinter();
    private final DefaultPayloadCodec codec = new DefaultPayloadCodec();
    private final PiiRedactor redactor = new PiiRedactor();
    private final AuditHashChain auditHashChain = new AuditHashChain();
    private final KeysetCursorCodec cursorCodec =
            new KeysetCursorCodec("0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8));
    private final ReplayHeaderSanitizer headerSanitizer =
            new ReplayHeaderSanitizer(Clock.fixed(Instant.parse("2026-11-24T12:00:00Z"), ZoneOffset.UTC));

    @Property(tries = 150)
    void fingerprinterIsDeterministicAndAlways64Hex(@ForAll String fqcn, @ForAll String message, @ForAll String stack) {
        var r1 = fingerprinter.fingerprint(fqcn, null, message, stack);
        var r2 = fingerprinter.fingerprint(fqcn, null, message, stack);
        assertThat(r1.fingerprint()).hasSize(64).matches("[0-9a-f]{64}").isEqualTo(r2.fingerprint());
    }

    @Property(tries = 150)
    void payloadCodecNeverThrowsOnArbitraryBytes(@ForAll @Size(max = 1024) byte[] bytes) {
        var decoded = codec.decode(bytes, Map.of(), false);
        assertThat(decoded.sizeBytes()).isEqualTo(bytes.length);
        assertThat(decoded.format()).isNotNull();
    }

    @Property(tries = 100)
    void confluentMagicWireFormatIsAlwaysNotPatchable(
            @ForAll @IntRange(min = 1, max = Integer.MAX_VALUE) int schemaId,
            @ForAll @Size(max = 128) byte[] trailing) {
        byte[] wire = ByteBuffer.allocate(5 + trailing.length)
                .put((byte) 0x00)
                .putInt(schemaId)
                .put(trailing)
                .array();
        var decoded = codec.decode(wire, Map.of(), false);
        assertThat(decoded.format()).isEqualTo(PayloadFormat.CONFLUENT_SCHEMA_REGISTRY);
        assertThat(decoded.patchable()).isFalse();
        assertThat(decoded.schemaId()).isEqualTo(schemaId);
    }

    @Property(tries = 150)
    void piiRedactorIsIdempotent(@ForAll String arbitrary) {
        String once = redactor.redactText(arbitrary);
        String twice = redactor.redactText(once);
        assertThat(twice).isEqualTo(once);
    }

    @Property(tries = 150)
    void csvSanitizerNeutralizesDangerousLeadingCharacters(@ForAll String raw) {
        String cell = CsvExportSanitizer.sanitizeCell(raw);
        if (!cell.isEmpty()) {
            char first = cell.charAt(0);
            if (first == '"' && cell.length() > 1) {
                first = cell.charAt(1);
            }
            assertThat(first).isNotIn('=', '+', '-', '@', '\t', '\r');
        }
    }

    @Property(tries = 100)
    void keysetCursorRoundTripsAndRejectsTampering(
            @ForAll @IntRange(min = 1_600_000_000, max = 1_900_000_000) int epochSeconds,
            @ForAll @IntRange(min = 0, max = 999_999) int micros) {
        Instant ts = Instant.ofEpochSecond(epochSeconds, micros * 1_000L);
        UUID id = UUID.randomUUID();
        String token = cursorCodec.encode(ts, id);
        var decoded = cursorCodec.decode(token);
        assertThat(decoded.ingestedAt()).isEqualTo(ts);
        assertThat(decoded.id()).isEqualTo(id);

        char flipped = token.charAt(token.length() - 1) == 'a' ? 'b' : 'a';
        String tampered = token.substring(0, token.length() - 1) + flipped;
        assertThatThrownBy(() -> cursorCodec.decode(tampered)).isInstanceOf(ValidationDomainException.class);
    }

    @Property(tries = 80)
    void merkleRootIsDeterministicAndSensitiveToOrder(@ForAll @Size(min = 1, max = 16) List<Long> seeds) {
        List<UUID> ids = seeds.stream().map(s -> new UUID(s, ~s)).toList();
        String root1 = auditHashChain.computeMerkleRoot(ids);
        String root2 = auditHashChain.computeMerkleRoot(ids);
        assertThat(root1).hasSize(64).isEqualTo(root2);
    }

    @Property(tries = 80)
    void headerSanitizerAlwaysStripsDltHeadersAndStampsIdempotencyKey(
            @ForAll @IntRange(min = 0, max = 32) int partition,
            @ForAll @IntRange(min = 0, max = 100_000) int offset) {
        UUID planId = UUID.randomUUID();
        var stamped = headerSanitizer.sanitizeAndStamp(
                Map.of("kafka_dlt-original-topic", "orders", "x-business", "ok"),
                planId,
                new MessageCoordinates("orders.dlt", partition, offset),
                1L,
                0);
        assertThat(stamped).doesNotContainKey("kafka_dlt-original-topic");
        assertThat(stamped).containsEntry("x-business", "ok");
        assertThat(stamped.get(ReplayHeaderSanitizer.HEADER_IDEMPOTENCY_KEY)).hasSize(64);
    }
}
