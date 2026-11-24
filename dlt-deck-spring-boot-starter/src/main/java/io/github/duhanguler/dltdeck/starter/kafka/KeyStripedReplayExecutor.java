package io.github.duhanguler.dltdeck.starter.kafka;

import io.github.duhanguler.dltdeck.core.jfr.DltIndeterminateRecoveryEvent;
import io.github.duhanguler.dltdeck.core.jfr.DltReplayRecordEvent;
import io.github.duhanguler.dltdeck.core.model.DltMessageDetail;
import io.github.duhanguler.dltdeck.core.model.DltMessageSummary;
import io.github.duhanguler.dltdeck.core.model.PlanStripeLease;
import io.github.duhanguler.dltdeck.core.model.ReplayOrderingMode;
import io.github.duhanguler.dltdeck.core.model.ReplayPlan;
import io.github.duhanguler.dltdeck.core.model.ReplayPlanStatus;
import io.github.duhanguler.dltdeck.core.patch.JsonPayloadPatcher;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.BlastRadiusGuard;
import io.github.duhanguler.dltdeck.core.policy.MakerCheckerPolicy;
import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import io.github.duhanguler.dltdeck.core.spi.AuditLedgerRepository;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import io.github.duhanguler.dltdeck.core.spi.ReplayPlanRepository;
import io.github.duhanguler.dltdeck.core.spi.ReplayRateLimitCoordinator;
import io.github.duhanguler.dltdeck.core.spi.StripeLeaseStore;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.resilience.annotation.ConcurrencyLimit;

/**
 * Multi-pod Key-Striped Virtual-Thread Replay Executor.
 *
 * <p>Implements every production invariant from the v3/v4 specification:
 * <ol>
 *   <li>Deterministic stripe leasing ({@code dlt_deck_plan_stripes}) guaranteeing 1 pod per stripe.</li>
 *   <li>Two short DB transactions separated by Kafka I/O: TX1 claims + increments {@code fencing_token} and commits
 *       immediately; Kafka publish runs with ZERO DB connection held; TX2 marks {@code PUBLISHED} fenced by {@code fencing_token}.</li>
 *   <li>Per-Key Head-of-Line Barrier ({@code BLOCKED_BY_PRIOR_KEY_FAILURE}) when an earlier event for key {@code K} fails or becomes {@code INDETERMINATE}.</li>
 *   <li>Expired {@code CLAIMED} leases strictly transition to {@code INDETERMINATE} (never silently double-replayed).</li>
 *   <li>Periodic SHA-256 Batch Merkle-Root Audit Checkpoints.</li>
 * </ol>
 */
public class KeyStripedReplayExecutor implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(KeyStripedReplayExecutor.class);

    private final KafkaTemplate<byte[], byte[]> kafkaTemplate;
    private final DltMessageRepository messageRepository;
    private final ReplayPlanRepository planRepository;
    private final StripeLeaseStore stripeLeaseStore;
    private final AuditLedgerRepository auditRepository;
    private final ReplayRateLimitCoordinator rateLimitCoordinator;
    private final TargetTopicValidator targetTopicValidator;
    private final JsonPayloadPatcher payloadPatcher;
    private final ReplayHeaderSanitizer headerSanitizer;
    private final MakerCheckerPolicy makerCheckerPolicy;
    private final BlastRadiusGuard blastRadiusGuard;
    private final AuditHashChain auditHashChain;
    private final DltDeckProperties properties;
    private final Clock clock;

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final ExecutorService stripeVirtualExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService sweeperScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> Thread.ofVirtual().name("dlt-deck-sweeper").unstarted(r));

    private volatile @Nullable FaultInjectionHook faultHook;

    public KeyStripedReplayExecutor(
            KafkaTemplate<byte[], byte[]> kafkaTemplate,
            DltMessageRepository messageRepository,
            ReplayPlanRepository planRepository,
            StripeLeaseStore stripeLeaseStore,
            AuditLedgerRepository auditRepository,
            ReplayRateLimitCoordinator rateLimitCoordinator,
            TargetTopicValidator targetTopicValidator,
            JsonPayloadPatcher payloadPatcher,
            ReplayHeaderSanitizer headerSanitizer,
            MakerCheckerPolicy makerCheckerPolicy,
            BlastRadiusGuard blastRadiusGuard,
            AuditHashChain auditHashChain,
            DltDeckProperties properties,
            Clock clock) {
        this.kafkaTemplate = kafkaTemplate;
        this.messageRepository = messageRepository;
        this.planRepository = planRepository;
        this.stripeLeaseStore = stripeLeaseStore;
        this.auditRepository = auditRepository;
        this.rateLimitCoordinator = rateLimitCoordinator;
        this.targetTopicValidator = targetTopicValidator;
        this.payloadPatcher = payloadPatcher;
        this.headerSanitizer = headerSanitizer;
        this.makerCheckerPolicy = makerCheckerPolicy;
        this.blastRadiusGuard = blastRadiusGuard;
        this.auditHashChain = auditHashChain;
        this.properties = properties;
        this.clock = clock;
    }

    public void setFaultHook(@Nullable FaultInjectionHook faultHook) {
        this.faultHook = faultHook;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        long sweepMs = Math.max(1000L, properties.getReplay().getIndeterminateSweepInterval().toMillis());
        var _ = sweeperScheduler.scheduleWithFixedDelay(this::sweepExpiredLeasesSafely, sweepMs, sweepMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        sweeperScheduler.shutdownNow();
        stripeVirtualExecutor.shutdown();
        try {
            if (!stripeVirtualExecutor.awaitTermination(15, TimeUnit.SECONDS)) {
                stripeVirtualExecutor.shutdownNow();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            stripeVirtualExecutor.shutdownNow();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 50;
    }

    /**
     * Executes an approved replay plan using deterministic multi-pod stripe leasing and Virtual Threads.
     */
    @ConcurrencyLimit(4)
    public ReplayPlan executePlan(UUID planId, String workerPodId) {
        ReplayPlan plan = planRepository
                .findById(planId)
                .orElseThrow(() -> new io.github.duhanguler.dltdeck.core.exception.NotFoundException(
                        "PLAN_NOT_FOUND", "Replay plan not found: " + planId));

        makerCheckerPolicy.verifyExecutionIntegrity(plan);
        targetTopicValidator.validateReplayTarget(plan.targetTopic());

        int stripeCount = Math.max(1, properties.getReplay().getStripeCount());
        stripeLeaseStore.initializeStripes(planId, stripeCount);
        planRepository.updateStatus(planId, ReplayPlanStatus.APPROVED, ReplayPlanStatus.RUNNING, null);

        int concurrency = Math.min(stripeCount, Math.max(1, properties.getReplay().getMaxConcurrentStripes()));
        AtomicInteger remainingBudget = new AtomicInteger(plan.maxRecords());
        List<Future<?>> futures = new ArrayList<>(concurrency);
        for (int i = 0; i < concurrency; i++) {
            int workerIdx = i;
            futures.add(stripeVirtualExecutor.submit(
                    () -> runStripeWorkerLoop(plan, workerPodId + "-w" + workerIdx, stripeCount, remainingBudget)));
        }
        for (Future<?> f : futures) {
            try {
                f.get();
            } catch (Exception ex) {
                if (ex instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                log.error("Stripe worker failed for plan {}", planId, ex);
            }
        }

        return finalizePlanIfAllStripesDone(planId);
    }

    private void runStripeWorkerLoop(
            ReplayPlan plan, String leaseOwner, int stripeCount, AtomicInteger remainingBudget) {
        UUID planId = plan.id();
        while (remainingBudget.get() > 0) {
            ReplayPlan latest = planRepository.findById(planId).orElse(null);
            if (latest == null || latest.status() != ReplayPlanStatus.RUNNING) {
                return;
            }
            Instant now = Instant.now(clock);
            Instant expiry = now.plus(properties.getReplay().getStripeLeaseDuration());
            Optional<PlanStripeLease> leaseOpt =
                    stripeLeaseStore.acquireNextStripe(planId, leaseOwner, expiry, now);
            if (leaseOpt.isEmpty()) {
                return;
            }
            PlanStripeLease lease = leaseOpt.get();
            processStripe(plan, lease, stripeCount, remainingBudget);
        }
    }

    private void processStripe(
            ReplayPlan plan, PlanStripeLease lease, int stripeCount, AtomicInteger remainingBudget) {
        UUID planId = plan.id();
        Set<String> blockedKeysInStripe = ConcurrentHashMap.newKeySet();
        Set<UUID> attemptedInRun = ConcurrentHashMap.newKeySet();
        List<UUID> merkleBatch = new ArrayList<>();
        int merkleBatchSize = Math.max(1, properties.getReplay().getMerkleCheckpointBatchSize());
        boolean stripePaused = false;
        boolean completedNormally = false;

        try {
            while (remainingBudget.get() > 0) {
                ReplayPlan latest = planRepository.findById(planId).orElse(null);
                if (latest == null || latest.status() != ReplayPlanStatus.RUNNING) {
                    stripePaused = true;
                    break;
                }
                if (blastRadiusGuard.evaluate(planId).tripped()) {
                    planRepository.updateStatus(
                            planId, ReplayPlanStatus.RUNNING, ReplayPlanStatus.PAUSED_BLAST_RADIUS, null);
                    stripeLeaseStore.pauseAllStripes(planId);
                    stripePaused = true;
                    break;
                }

                List<DltMessageSummary> candidates = messageRepository.findEligibleForStripe(
                        plan.dltTopicFilter(),
                        plan.fingerprintFilter(),
                        stripeCount,
                        lease.stripeId(),
                        Math.min(50, Math.max(1, remainingBudget.get())));
                if (candidates.isEmpty()) {
                    break;
                }

                boolean madeProgress = false;
                for (DltMessageSummary candidate : candidates) {
                    if (remainingBudget.get() <= 0) {
                        break;
                    }
                    if (!attemptedInRun.add(candidate.id())) {
                        continue;
                    }
                    String keyFp = candidate.keyFingerprint();
                    if (plan.orderingMode() == ReplayOrderingMode.PRESERVE_KEY_ORDER
                            && keyFp != null
                            && blockedKeysInStripe.contains(keyFp)) {
                        int blocked = messageRepository.blockSubsequentForKey(
                                planId, candidate.dltTopic(), keyFp, null, Instant.now(clock));
                        if (blocked > 0) {
                            planRepository.incrementCounters(planId, 0, 0, 0, 0, blocked, 0);
                            madeProgress = true;
                        }
                        continue;
                    }
                    madeProgress = true;

                    // Renew stripe lease proactively
                    stripeLeaseStore.renewLease(
                            planId,
                            lease.stripeId(),
                            lease.leaseOwner() == null ? "" : lease.leaseOwner(),
                            lease.fencingToken(),
                            Instant.now(clock).plus(properties.getReplay().getStripeLeaseDuration()));

                    // Acquire global/local rate-limit permit
                    acquireRateLimitPermit(planId, plan.rateLimitPerSec());

                    // Short TX1: claim + fencing_token++ (commits IMMEDIATELY before Kafka I/O!)
                    Instant claimNow = Instant.now(clock);
                    Instant msgExpiry = claimNow.plus(properties.getReplay().getMessageLeaseDuration());
                    Optional<DltMessageRepository.ClaimedMessage> claimedOpt = messageRepository.claimForReplay(
                            candidate.id(),
                            planId,
                            lease.leaseOwner() == null ? properties.getPodId() : lease.leaseOwner(),
                            msgExpiry,
                            claimNow);
                    if (claimedOpt.isEmpty()) {
                        continue;
                    }
                    remainingBudget.decrementAndGet();
                    planRepository.incrementCounters(planId, 1, 0, 0, 0, 0, 0);

                    DltMessageRepository.ClaimedMessage claimed = claimedOpt.get();
                    DltMessageDetail detail = claimed.detail();
                    long fencingToken = claimed.fencingToken();

                    ReplayOutcome outcome = publishAndFinalizeTx2(plan, detail, fencingToken, lease.stripeId());
                    if (outcome == ReplayOutcome.PUBLISHED) {
                        merkleBatch.add(detail.summary().id());
                        blastRadiusGuard.recordPublished(planId);
                        if (merkleBatch.size() >= merkleBatchSize) {
                            flushMerkleCheckpoint(planId, merkleBatch);
                            merkleBatch.clear();
                        }
                    } else if (outcome == ReplayOutcome.FAILED_PUBLISH || outcome == ReplayOutcome.INDETERMINATE_CRASH) {
                        if (plan.orderingMode() == ReplayOrderingMode.PRESERVE_KEY_ORDER && keyFp != null) {
                            blockedKeysInStripe.add(keyFp);
                            int blocked = messageRepository.blockSubsequentForKey(
                                    planId, detail.summary().dltTopic(), keyFp, detail.summary().id(), Instant.now(clock));
                            if (blocked > 0) {
                                planRepository.incrementCounters(planId, 0, 0, 0, 0, blocked, 0);
                            }
                        }
                    }
                }
                if (!madeProgress) {
                    break;
                }
            }
            completedNormally = true;
        } finally {
            if (!merkleBatch.isEmpty()) {
                flushMerkleCheckpoint(planId, merkleBatch);
            }
            if (completedNormally && !stripePaused && lease.leaseOwner() != null) {
                stripeLeaseStore.markCompleted(planId, lease.stripeId(), lease.leaseOwner(), lease.fencingToken());
            }
        }
    }

    private ReplayOutcome publishAndFinalizeTx2(
            ReplayPlan plan, DltMessageDetail detail, long fencingToken, int stripeId) {
        DltMessageSummary summary = detail.summary();
        UUID planId = plan.id();
        String outcomeLabel = "FAILED_PUBLISH";
        try {
            byte[] payloadToSend = detail.payload();
            if (plan.payloadPatchJson() != null && !plan.payloadPatchJson().isBlank()) {
                payloadToSend = payloadPatcher
                        .applyMergePatch(summary.payloadFormat(), payloadToSend, plan.payloadPatchJson())
                        .patchedBytes();
            }

            Map<String, String> stampedHeaders = headerSanitizer.sanitizeAndStamp(
                    detail.headers(), planId, summary.dltCoordinates(), fencingToken, summary.replayHops());

            ProducerRecord<byte[], byte[]> producerRecord =
                    new ProducerRecord<>(plan.targetTopic(), detail.messageKey(), payloadToSend);
            for (Map.Entry<String, String> h : stampedHeaders.entrySet()) {
                producerRecord.headers().add(new RecordHeader(h.getKey(), h.getValue().getBytes(StandardCharsets.UTF_8)));
            }

            FaultInjectionHook hook = faultHook;
            if (hook != null) {
                hook.beforeKafkaSend(detail, fencingToken);
            }

            // Kafka I/O executes strictly OUTSIDE any database transaction!
            SendResult<byte[], byte[]> sendResult = kafkaTemplate.send(producerRecord).get(10, TimeUnit.SECONDS);
            RecordMetadata metadata = sendResult.getRecordMetadata();

            if (hook != null && hook.simulateCrashAfterBrokerAckBeforeTx2(detail, fencingToken)) {
                // Simulated pod crash after broker ACK and before TX2 DB commit:
                // Leave row CLAIMED so expired lease sweeper transitions it to INDETERMINATE!
                outcomeLabel = "CRASHED_BEFORE_TX2";
                return ReplayOutcome.INDETERMINATE_CRASH;
            }

            // Short TX2: fenced commit by fencing_token
            boolean marked = messageRepository.markPublished(
                    summary.id(), fencingToken, metadata.partition(), metadata.offset(), Instant.now(clock));
            if (marked) {
                planRepository.incrementCounters(planId, 0, 1, 0, 0, 0, 0);
                outcomeLabel = "PUBLISHED";
                return ReplayOutcome.PUBLISHED;
            }
            outcomeLabel = "FENCE_LOST";
            return ReplayOutcome.INDETERMINATE_CRASH;
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Replay publish failed for message {} (plan {}): {}", summary.id(), planId, ex.getMessage());
            messageRepository.markFailedPublish(summary.id(), fencingToken, Instant.now(clock));
            planRepository.incrementCounters(planId, 0, 0, 1, 0, 0, 0);
            return ReplayOutcome.FAILED_PUBLISH;
        } finally {
            DltReplayRecordEvent jfr = new DltReplayRecordEvent();
            if (jfr.isEnabled()) {
                jfr.planId = planId.toString();
                jfr.messageId = summary.id().toString();
                jfr.stripeId = stripeId;
                jfr.targetTopic = plan.targetTopic();
                jfr.fencingToken = fencingToken;
                jfr.outcomeStatus = outcomeLabel;
                jfr.commit();
            }
        }
    }

    private void flushMerkleCheckpoint(UUID planId, List<UUID> publishedIds) {
        String merkleRoot = auditHashChain.computeMerkleRoot(publishedIds);
        String details = "{\"batchSize\":" + publishedIds.size() + ",\"firstId\":\"" + publishedIds.getFirst()
                + "\",\"lastId\":\"" + publishedIds.getLast() + "\"}";
        auditRepository.append(
                UUID.randomUUID(),
                properties.getPodId(),
                "SYSTEM",
                "BATCH_MERKLE_CHECKPOINT",
                "REPLAY_PLAN",
                planId.toString(),
                merkleRoot,
                details,
                Instant.now(clock));
    }

    private ReplayPlan finalizePlanIfAllStripesDone(UUID planId) {
        List<PlanStripeLease> stripes = stripeLeaseStore.findStripes(planId);
        boolean allCompleted = !stripes.isEmpty()
                && stripes.stream().allMatch(s -> s.status() == PlanStripeLease.StripeStatus.COMPLETED);
        ReplayPlan current = planRepository.findById(planId).orElseThrow();
        if (allCompleted && current.status() == ReplayPlanStatus.RUNNING) {
            ReplayPlanStatus terminal = (current.totalFailed() > 0
                            || current.totalIndeterminate() > 0
                            || current.totalBlockedHol() > 0)
                    ? ReplayPlanStatus.COMPLETED_WITH_FAILURES
                    : ReplayPlanStatus.COMPLETED;
            planRepository.updateStatus(planId, ReplayPlanStatus.RUNNING, terminal, Instant.now(clock));
            return planRepository.findById(planId).orElse(current);
        }
        return current;
    }

    /**
     * Sweeps expired {@code CLAIMED} leases strictly to {@code INDETERMINATE} (never silently re-publishing!).
     */
    public List<DltMessageSummary> sweepExpiredLeases() {
        Instant now = Instant.now(clock);
        List<DltMessageSummary> swept = messageRepository.sweepExpiredLeasesToIndeterminate(now, 200);
        for (DltMessageSummary msg : swept) {
            if (msg.activePlanId() != null) {
                planRepository.incrementCounters(msg.activePlanId(), 0, 0, 0, 1, 0, 0);
            }
            DltIndeterminateRecoveryEvent jfr = new DltIndeterminateRecoveryEvent();
            if (jfr.isEnabled()) {
                jfr.messageId = msg.id().toString();
                jfr.previousLeaseOwner = msg.leaseOwner();
                jfr.fencingToken = msg.fencingToken();
                jfr.action = "SWEPT_TO_INDETERMINATE";
                jfr.commit();
            }
        }
        return swept;
    }

    private void sweepExpiredLeasesSafely() {
        try {
            sweepExpiredLeases();
        } catch (Exception ex) {
            log.debug("Indeterminate lease sweep error: {}", ex.getMessage());
        }
    }

    private void acquireRateLimitPermit(UUID planId, int rateLimitPerSec) {
        if (rateLimitPerSec <= 0) {
            return;
        }
        int attempts = 0;
        while (!rateLimitCoordinator.tryAcquire(planId, rateLimitPerSec) && attempts < 200) {
            attempts++;
            Duration wait = rateLimitCoordinator.suggestedWait(planId, rateLimitPerSec);
            try {
                Thread.sleep(Math.min(wait.toMillis(), 250L));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private enum ReplayOutcome {
        PUBLISHED,
        FAILED_PUBLISH,
        INDETERMINATE_CRASH
    }

    /**
     * Fault-injection hook used exclusively by fault-injection integration tests to deterministically
     * simulate broker failures or pod crashes between Kafka broker ACK and TX2 DB commit.
     */
    public interface FaultInjectionHook {
        default void beforeKafkaSend(DltMessageDetail detail, long fencingToken) {}

        default boolean simulateCrashAfterBrokerAckBeforeTx2(DltMessageDetail detail, long fencingToken) {
            return false;
        }
    }
}
