package io.github.duhanguler.dltdeck.core.policy;

import io.github.duhanguler.dltdeck.core.jfr.DltBlastRadiusTripEvent;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Gatherers;

/**
 * Clock-driven sliding-window blast-radius circuit breaker using Java 25 final
 * {@link Gatherers#windowSliding(int)} (JEP 485).
 *
 * <p>Tracks replay publish vs. correlated re-DLT outcomes per plan and trips
 * {@code PAUSED_BLAST_RADIUS} when the sliding window's failure ratio breaches the configured threshold.
 */
public final class BlastRadiusGuard {

    private final Clock clock;
    private final int windowSize;
    private final double maxReDltRatio;
    private final Duration retentionWindow;
    private final Map<UUID, PlanWindow> windows = new ConcurrentHashMap<>();

    public BlastRadiusGuard(Clock clock, int windowSize, double maxReDltRatio, Duration retentionWindow) {
        if (windowSize < 2) {
            throw new IllegalArgumentException("windowSize must be >= 2");
        }
        if (maxReDltRatio <= 0.0 || maxReDltRatio > 1.0) {
            throw new IllegalArgumentException("maxReDltRatio must be in (0.0, 1.0]");
        }
        if (retentionWindow.isNegative() || retentionWindow.isZero()) {
            throw new IllegalArgumentException("retentionWindow must be positive");
        }
        this.clock = clock;
        this.windowSize = windowSize;
        this.maxReDltRatio = maxReDltRatio;
        this.retentionWindow = retentionWindow;
    }

    public BlastRadiusDecision recordPublished(UUID planId) {
        return recordOutcome(planId, false);
    }

    public BlastRadiusDecision recordCorrelatedReDlt(UUID planId) {
        return recordOutcome(planId, true);
    }

    public BlastRadiusDecision evaluate(UUID planId) {
        PlanWindow planWindow = windows.get(planId);
        if (planWindow == null) {
            return new BlastRadiusDecision(false, 0.0, maxReDltRatio, 0);
        }
        return planWindow.evaluate(planId, Instant.now(clock), retentionWindow, windowSize, maxReDltRatio);
    }

    public void reset(UUID planId) {
        windows.remove(planId);
    }

    private BlastRadiusDecision recordOutcome(UUID planId, boolean reDlt) {
        PlanWindow planWindow = windows.computeIfAbsent(planId, _ -> new PlanWindow());
        return planWindow.addAndEvaluate(
                planId, Instant.now(clock), reDlt, retentionWindow, windowSize, maxReDltRatio);
    }

    private record OutcomeSample(Instant timestamp, boolean reDlt) {}

    private static final class PlanWindow {
        private final ReentrantLock lock = new ReentrantLock();
        private final List<OutcomeSample> samples = new ArrayList<>();

        BlastRadiusDecision addAndEvaluate(
                UUID planId,
                Instant now,
                boolean reDlt,
                Duration retentionWindow,
                int windowSize,
                double maxRatio) {
            lock.lock();
            try {
                samples.add(new OutcomeSample(now, reDlt));
                return evaluateLocked(planId, now, retentionWindow, windowSize, maxRatio);
            } finally {
                lock.unlock();
            }
        }

        BlastRadiusDecision evaluate(
                UUID planId, Instant now, Duration retentionWindow, int windowSize, double maxRatio) {
            lock.lock();
            try {
                return evaluateLocked(planId, now, retentionWindow, windowSize, maxRatio);
            } finally {
                lock.unlock();
            }
        }

        private BlastRadiusDecision evaluateLocked(
                UUID planId, Instant now, Duration retentionWindow, int windowSize, double maxRatio) {
            Instant cutoff = now.minus(retentionWindow);
            samples.removeIf(sample -> sample.timestamp().isBefore(cutoff));
            int maxHistory = windowSize * 4;
            if (samples.size() > maxHistory) {
                samples.subList(0, samples.size() - maxHistory).clear();
            }

            if (samples.size() < windowSize) {
                long failures = samples.stream().filter(OutcomeSample::reDlt).count();
                double ratio = samples.isEmpty() ? 0.0 : (double) failures / samples.size();
                return new BlastRadiusDecision(false, ratio, maxRatio, samples.size());
            }

            List<List<OutcomeSample>> slidingWindows =
                    samples.stream().gather(Gatherers.windowSliding(windowSize)).toList();

            double peakRatio = 0.0;
            for (List<OutcomeSample> w : slidingWindows) {
                long failures = w.stream().filter(OutcomeSample::reDlt).count();
                double ratio = (double) failures / w.size();
                if (ratio > peakRatio) {
                    peakRatio = ratio;
                }
            }

            List<OutcomeSample> latest = slidingWindows.getLast();
            long latestFailures = latest.stream().filter(OutcomeSample::reDlt).count();
            double latestRatio = (double) latestFailures / latest.size();
            boolean tripped = latestRatio >= maxRatio;

            if (tripped) {
                DltBlastRadiusTripEvent jfr = new DltBlastRadiusTripEvent();
                if (jfr.isEnabled()) {
                    jfr.planId = planId.toString();
                    jfr.failureRatio = latestRatio;
                    jfr.thresholdRatio = maxRatio;
                    jfr.windowSize = windowSize;
                    jfr.commit();
                }
            }

            return new BlastRadiusDecision(tripped, Math.max(latestRatio, peakRatio), maxRatio, latest.size());
        }
    }

    public record BlastRadiusDecision(
            boolean tripped, double observedRatio, double thresholdRatio, int sampleCount) {}
}
