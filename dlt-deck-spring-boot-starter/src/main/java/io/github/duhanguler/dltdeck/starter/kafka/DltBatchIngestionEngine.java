package io.github.duhanguler.dltdeck.starter.kafka;

import io.github.duhanguler.dltdeck.core.codec.PayloadCodec;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.jfr.DltIngestionBatchEvent;
import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.policy.BlastRadiusGuard;
import io.github.duhanguler.dltdeck.core.policy.TargetTopicGuard;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import io.github.duhanguler.dltdeck.core.spi.ReplayPlanRepository;
import io.github.duhanguler.dltdeck.core.spi.StripeLeaseStore;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Gatherers;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Isolated Kafka 4.4 KIP-848 batch DLT ingestion engine implementing {@link SmartLifecycle} for clean drain & commit.
 *
 * <p>Guarantees:
 * <ul>
 *   <li>Uses {@code ByteArrayDeserializer} and {@code enable.auto.commit=false}.</li>
 *   <li>Groups records via Java 25 {@link Gatherers#windowFixed(int)} (JEP 485) and persists via JDBC batching.</li>
 *   <li>Commits Kafka offsets strictly AFTER database transaction commit.</li>
 *   <li>Pauses/resumes partitions on DB write-latency backpressure.</li>
 *   <li>Computes live {@code ingestionLag} ({@code endOffsets - currentPosition}) per topic and globally.</li>
 * </ul>
 */
public final class DltBatchIngestionEngine implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(DltBatchIngestionEngine.class);
    private static final HexFormat LOWER_HEX = HexFormat.of();

    private final Map<String, Object> baseConsumerProps;
    private final AdminClient adminClient;
    private final DltDeckProperties properties;
    private final SafeSpringHeaderExtractor headerExtractor;
    private final PayloadCodec payloadCodec;
    private final ExceptionFingerprinter fingerprinter;
    private final DltMessageRepository messageRepository;
    private final ReplayPlanRepository planRepository;
    private final StripeLeaseStore stripeLeaseStore;
    private final BlastRadiusGuard blastRadiusGuard;
    private final Clock clock;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicLong totalLag = new AtomicLong(0L);
    private final AtomicLong totalIngestedCount = new AtomicLong(0L);
    private final Map<String, Long> topicLag = new ConcurrentHashMap<>();
    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();

    private volatile @Nullable Consumer<byte[], byte[]> activeConsumer;
    private volatile CountDownLatch shutdownLatch = new CountDownLatch(0);

    public DltBatchIngestionEngine(
            Map<String, Object> baseConsumerProps,
            AdminClient adminClient,
            DltDeckProperties properties,
            SafeSpringHeaderExtractor headerExtractor,
            PayloadCodec payloadCodec,
            ExceptionFingerprinter fingerprinter,
            DltMessageRepository messageRepository,
            ReplayPlanRepository planRepository,
            StripeLeaseStore stripeLeaseStore,
            BlastRadiusGuard blastRadiusGuard,
            Clock clock) {
        this.baseConsumerProps = Map.copyOf(baseConsumerProps);
        this.adminClient = adminClient;
        this.properties = properties;
        this.headerExtractor = headerExtractor;
        this.payloadCodec = payloadCodec;
        this.fingerprinter = fingerprinter;
        this.messageRepository = messageRepository;
        this.planRepository = planRepository;
        this.stripeLeaseStore = stripeLeaseStore;
        this.blastRadiusGuard = blastRadiusGuard;
        this.clock = clock;
    }

    @Override
    public void start() {
        if (!properties.getIngestion().isEnabled()) {
            log.info("spring-dlt-deck Kafka ingestion is disabled via configuration");
            return;
        }
        if (!running.compareAndSet(false, true)) {
            return;
        }
        shutdownLatch = new CountDownLatch(1);
        Thread.ofVirtual().name("dlt-deck-ingestor").start(this::runLoop);
        log.info("Started spring-dlt-deck KIP-848 batch ingestion engine (group={})", properties.getIngestion().getGroupId());
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        Consumer<byte[], byte[]> consumer = activeConsumer;
        if (consumer != null) {
            consumer.wakeup();
        }
        try {
            if (!shutdownLatch.await(15, TimeUnit.SECONDS)) {
                log.warn("Timed out waiting for spring-dlt-deck ingestion loop to drain cleanly");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    public long totalIngestionLag() {
        return totalLag.get();
    }

    public Map<String, Long> ingestionLagByTopic() {
        return Map.copyOf(topicLag);
    }

    public Set<String> subscribedTopics() {
        return Set.copyOf(subscribedTopics);
    }

    public long totalIngestedCount() {
        return totalIngestedCount.get();
    }

    private void runLoop() {
        Consumer<byte[], byte[]> consumer = null;
        try {
            consumer = createConsumerWithFallback();
            activeConsumer = consumer;

            Instant nextDiscoveryAt = Instant.EPOCH;
            Instant pausedUntil = Instant.EPOCH;
            boolean paused = false;

            while (running.get()) {
                Instant now = Instant.now(clock);
                if (!now.isBefore(nextDiscoveryAt)) {
                    refreshSubscriptions(consumer);
                    nextDiscoveryAt = now.plus(properties.getIngestion().getDiscoveryInterval());
                }

                if (subscribedTopics.isEmpty()) {
                    sleepQuietly(250);
                    continue;
                }

                if (paused && !now.isBefore(pausedUntil)) {
                    Set<TopicPartition> assignment = consumer.assignment();
                    if (!assignment.isEmpty()) {
                        consumer.resume(assignment);
                        log.info("Resumed spring-dlt-deck ingestion after DB backpressure window");
                    }
                    paused = false;
                }

                ConsumerRecords<byte[], byte[]> records = consumer.poll(properties.getIngestion().getPollTimeout());
                if (!records.isEmpty()) {
                    long startNs = System.nanoTime();
                    processPolledRecords(consumer, records);
                    Duration dbLatency = Duration.ofNanos(System.nanoTime() - startNs);
                    if (dbLatency.compareTo(properties.getIngestion().getDbBackpressureThreshold()) > 0
                            && !consumer.assignment().isEmpty()) {
                        consumer.pause(consumer.assignment());
                        paused = true;
                        pausedUntil = Instant.now(clock).plus(properties.getIngestion().getBackpressurePauseDuration());
                        log.warn(
                                "DB batch write took {} ms (> threshold); pausing partitions for {}",
                                dbLatency.toMillis(),
                                properties.getIngestion().getBackpressurePauseDuration());
                    }
                }
                refreshLagMetrics(consumer);
            }
        } catch (WakeupException expectedOnShutdown) {
            if (running.get()) {
                log.warn("Unexpected WakeupException in ingestion loop", expectedOnShutdown);
            }
        } catch (Exception ex) {
            log.error("Fatal error in spring-dlt-deck ingestion loop", ex);
        } finally {
            if (consumer != null) {
                try {
                    consumer.close(Duration.ofSeconds(5));
                } catch (Exception ex) {
                    log.warn("Error closing spring-dlt-deck Kafka consumer", ex);
                }
            }
            activeConsumer = null;
            running.set(false);
            shutdownLatch.countDown();
            log.info("Stopped spring-dlt-deck ingestion engine cleanly");
        }
    }

    private Consumer<byte[], byte[]> createConsumerWithFallback() {
        Map<String, Object> props = new LinkedHashMap<>(baseConsumerProps);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getIngestion().getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getIngestion().getMaxPollRecords());

        String requestedProtocol = properties.getIngestion().getGroupProtocol();
        if (requestedProtocol != null && !requestedProtocol.isBlank()) {
            props.put("group.protocol", requestedProtocol);
        }
        try {
            return new KafkaConsumer<>(props);
        } catch (Exception ex) {
            log.warn("Falling back from group.protocol={} to classic consumer protocol: {}", requestedProtocol, ex.getMessage());
            props.put("group.protocol", "classic");
            return new KafkaConsumer<>(props);
        }
    }

    private void refreshSubscriptions(Consumer<byte[], byte[]> consumer) {
        try {
            Set<String> allTopics = adminClient.listTopics().names().get(5, TimeUnit.SECONDS);
            Set<String> matched = discoverEligibleDltTopics(allTopics);
            if (!matched.equals(subscribedTopics)) {
                subscribedTopics.clear();
                subscribedTopics.addAll(matched);
                if (!matched.isEmpty()) {
                    consumer.subscribe(List.copyOf(matched));
                    log.info("spring-dlt-deck subscribed to DLT topics: {}", matched);
                }
            }
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.debug("Topic discovery skipped due to transient Kafka metadata error: {}", ex.getMessage());
        }
    }

    public Set<String> discoverEligibleDltTopics(Set<String> clusterTopics) {
        Set<String> allow = Set.copyOf(properties.getIngestion().getAllowTopics());
        Set<String> deny = Set.copyOf(properties.getIngestion().getDenyTopics());
        List<Pattern> patterns = properties.getIngestion().getTopicPatterns().stream()
                .map(Pattern::compile)
                .toList();

        Set<String> eligible = new LinkedHashSet<>();
        for (String topic : clusterTopics) {
            if (deny.contains(topic) || TargetTopicGuard.isExcludedFromDiscovery(topic)) {
                continue;
            }
            if (!allow.isEmpty()) {
                if (allow.contains(topic)) {
                    eligible.add(topic);
                }
                continue;
            }
            for (Pattern p : patterns) {
                if (p.matcher(topic).matches()) {
                    eligible.add(topic);
                    break;
                }
            }
        }
        return eligible;
    }

    private void processPolledRecords(
            Consumer<byte[], byte[]> consumer, ConsumerRecords<byte[], byte[]> records) {
        List<ConsumerRecord<byte[], byte[]>> allRecords = new ArrayList<>(records.count());
        records.forEach(allRecords::add);

        int windowSize = Math.max(1, properties.getIngestion().getBatchWindowSize());
        List<List<ConsumerRecord<byte[], byte[]>>> batches =
                allRecords.stream().gather(Gatherers.windowFixed(windowSize)).toList();

        for (List<ConsumerRecord<byte[], byte[]>> batch : batches) {
            Instant now = Instant.now(clock);
            List<IngestedDltMessage> mapped = new ArrayList<>(batch.size());
            Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new LinkedHashMap<>();
            int quarantinedCount = 0;
            int reDltCount = 0;

            for (ConsumerRecord<byte[], byte[]> rec : batch) {
                IngestedDltMessage msg = mapRecord(rec, now);
                mapped.add(msg);
                if (msg.initialStatus() == MessageStatus.QUARANTINED) {
                    quarantinedCount++;
                }
                if (msg.correlatedPlanId() != null) {
                    reDltCount++;
                    handleCorrelatedReDlt(msg, now);
                }
                offsetsToCommit.put(
                        new TopicPartition(rec.topic(), rec.partition()),
                        new OffsetAndMetadata(rec.offset() + 1));
            }

            // Strictly DB commit BEFORE Kafka offset commit!
            int inserted = messageRepository.ingestBatch(mapped);
            totalIngestedCount.addAndGet(inserted);
            consumer.commitSync(offsetsToCommit);

            DltIngestionBatchEvent jfr = new DltIngestionBatchEvent();
            if (jfr.isEnabled()) {
                jfr.dltTopic = batch.getFirst().topic();
                jfr.batchSize = batch.size();
                jfr.quarantinedCount = quarantinedCount;
                jfr.reDltCount = reDltCount;
                jfr.ingestionLag = totalLag.get();
                jfr.commit();
            }
        }
    }

    public IngestedDltMessage mapRecord(ConsumerRecord<byte[], byte[]> rec, Instant now) {
        SafeSpringHeaderExtractor.ExtractedMetadata meta = headerExtractor.extract(rec);
        PayloadCodec.DecodedPayload decoded = payloadCodec.decode(
                meta.recoveredPayload(), meta.headers(), meta.hasDeserializationException());

        ExceptionFingerprinter.FingerprintResult fp = fingerprinter.fingerprint(
                meta.exceptionFqcn(),
                meta.exceptionCauseFqcn(),
                meta.exceptionMessage(),
                meta.exceptionStacktrace());

        boolean loopExceeded = meta.replayHops() >= properties.getIngestion().getMaxReplayHops();
        FailureCategory category = loopExceeded ? FailureCategory.REPLAY_LOOP : fp.failureCategory();
        MessageStatus initialStatus = loopExceeded ? MessageStatus.QUARANTINED : MessageStatus.NEW;
        String recommendation = loopExceeded ? FailureCategory.REPLAY_LOOP.defaultRecommendation() : fp.recommendation();

        byte[] keyBytes = meta.recoveredKey();
        int keyHash = keyBytes == null ? 0 : Arrays.hashCode(keyBytes);
        String keyFingerprint = keyBytes == null ? null : sha256Bytes(keyBytes);

        return new IngestedDltMessage(
                UUID.randomUUID(),
                rec.topic(),
                rec.partition(),
                rec.offset(),
                meta.originalTopic(),
                meta.originalPartition(),
                meta.originalOffset(),
                meta.originalConsumerGroup(),
                keyBytes,
                keyHash,
                keyFingerprint,
                meta.recoveredPayload(),
                decoded.format(),
                decoded.schemaId(),
                decoded.sizeBytes(),
                fp.fingerprint(),
                fp.exceptionFqcn(),
                fp.rootCauseFqcn(),
                fp.sampleMessage(),
                fp.canonicalStacktrace(),
                category,
                recommendation,
                meta.replayHops(),
                initialStatus,
                meta.correlatedPlanId(),
                meta.headers(),
                now);
    }

    private void handleCorrelatedReDlt(IngestedDltMessage msg, Instant now) {
        UUID planId = msg.correlatedPlanId();
        if (planId == null) {
            return;
        }
        planRepository.incrementCounters(planId, 0, 0, 0, 0, 0, 1);
        var decision = blastRadiusGuard.recordCorrelatedReDlt(planId);
        if (decision.tripped()) {
            boolean paused = planRepository.updateStatus(
                    planId,
                    io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus.RUNNING,
                    io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus.PAUSED_BLAST_RADIUS,
                    null);
            if (!paused) {
                planRepository.updateStatus(
                        planId,
                        io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus.APPROVED,
                        io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus.PAUSED_BLAST_RADIUS,
                        null);
            }
            stripeLeaseStore.pauseAllStripes(planId);
            log.warn(
                    "Blast-radius circuit tripped for plan {}: ratio={} >= {}",
                    planId,
                    decision.observedRatio(),
                    decision.thresholdRatio());
        }
        String origin = msg.headers().get("x-dlt-deck-origin-coords");
        if (origin != null) {
            parseAndMarkReDlt(origin, now);
        }
    }

    private void parseAndMarkReDlt(String originCoords, Instant now) {
        // Format: topic[partition]@offset
        int lb = originCoords.lastIndexOf('[');
        int rb = originCoords.lastIndexOf("]@");
        if (lb > 0 && rb > lb) {
            try {
                String topic = originCoords.substring(0, lb);
                int partition = Integer.parseInt(originCoords.substring(lb + 1, rb));
                long offset = Long.parseLong(originCoords.substring(rb + 2));
                messageRepository.markCorrelatedReDlt(topic, partition, offset, now);
            } catch (NumberFormatException ignored) {
                // Ignore malformed origin header
            }
        }
    }

    private void refreshLagMetrics(Consumer<byte[], byte[]> consumer) {
        Set<TopicPartition> assignment = consumer.assignment();
        if (assignment.isEmpty()) {
            totalLag.set(0L);
            return;
        }
        try {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignment, Duration.ofSeconds(2));
            Map<String, Long> perTopic = new LinkedHashMap<>();
            long sum = 0L;
            for (TopicPartition tp : assignment) {
                long end = endOffsets.getOrDefault(tp, 0L);
                long pos = consumer.position(tp, Duration.ofSeconds(2));
                long lag = Math.max(0L, end - pos);
                sum += lag;
                perTopic.merge(tp.topic(), lag, Long::sum);
            }
            topicLag.clear();
            topicLag.putAll(perTopic);
            totalLag.set(sum);
        } catch (Exception ex) {
            log.debug("Could not refresh ingestion lag: {}", ex.getMessage());
        }
    }

    private static String sha256Bytes(byte[] bytes) {
        try {
            return LOWER_HEX.formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
