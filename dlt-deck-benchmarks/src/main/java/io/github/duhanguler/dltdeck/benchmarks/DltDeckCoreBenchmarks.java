package io.github.duhanguler.dltdeck.benchmarks;

import io.github.duhanguler.dltdeck.core.codec.DefaultPayloadCodec;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
@State(Scope.Benchmark)
public class DltDeckCoreBenchmarks {

    private final ExceptionFingerprinter fingerprinter = new ExceptionFingerprinter();
    private final PiiRedactor piiRedactor = new PiiRedactor();
    private final AuditHashChain auditHashChain = new AuditHashChain();
    private final DefaultPayloadCodec payloadCodec = new DefaultPayloadCodec();

    private final String sampleStack = """
            org.springframework.kafka.listener.ListenerExecutionFailedException: Listener failed
                at com.acme.payment.PaymentHandler$$SpringCGLIB$$1234.handle(PaymentHandler.java:42)
                at com.acme.payment.OrderProcessor.process(OrderProcessor.java:88)
            Caused by: jakarta.validation.ConstraintViolationException: Order 550e8400-e29b-41d4-a716-446655440000 at 2026-11-24T19:00:00Z failed amount=12345 @deadbeef
                at com.acme.payment.Validator.check(Validator.java:19)
            """;

    private final String samplePiiJson = """
            {"orderId":"ORD-99100","tckn":"10000000146","iban":"TR330006100519786457841326","card":"4532-0151-1283-0366","email":"customer@example.com","phone":"+90 532 123 45 67","amount":149.90}
            """;

    private final byte[] jsonBytes = samplePiiJson.getBytes(StandardCharsets.UTF_8);
    private final byte[] confluentBytes = ByteBuffer.allocate(64).put((byte) 0x00).putInt(42).array();
    private final List<UUID> merkleBatch50 = IntStream.range(0, 50)
            .mapToObj(i -> new UUID(0x1000L + i, 0x2000L + i))
            .toList();

    @Benchmark
    public ExceptionFingerprinter.FingerprintResult fingerprintException() {
        return fingerprinter.fingerprint(
                "org.springframework.kafka.listener.ListenerExecutionFailedException",
                null,
                "Order 550e8400-e29b-41d4-a716-446655440000 failed amount=98765",
                sampleStack);
    }

    @Benchmark
    public String redactChecksumVerifiedPii() {
        return piiRedactor.redactText(samplePiiJson);
    }

    @Benchmark
    public String computeMerkleRoot50Records() {
        return auditHashChain.computeMerkleRoot(merkleBatch50);
    }

    @Benchmark
    public DefaultPayloadCodec.DecodedPayload sniffJsonPayload() {
        return payloadCodec.decode(jsonBytes, Map.of(), false);
    }

    @Benchmark
    public DefaultPayloadCodec.DecodedPayload sniffConfluentBinaryPayload() {
        return payloadCodec.decode(confluentBytes, Map.of(), false);
    }
}
