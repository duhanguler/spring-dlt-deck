package io.github.duhanguler.dltdeck.core.policy;

import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.model.AuditChainVerification;
import io.github.duhanguler.dltdeck.core.model.AuditEntry;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import org.jspecify.annotations.Nullable;

/**
 * Cryptographic SHA-256 tamper-evident audit hash chain and batch Merkle-root calculator.
 * Truncates timestamps to microseconds to guarantee bit-exact round-trip parity with PostgreSQL {@code TIMESTAMPTZ}.
 */
public final class AuditHashChain {

    public static final String GENESIS_HASH = "0".repeat(64);
    private static final HexFormat LOWER_HEX = HexFormat.of();

    public String computeEntryHash(
            String prevHash,
            UUID eventId,
            String actor,
            String actorRole,
            String action,
            String targetType,
            String targetId,
            @Nullable String merkleRoot,
            String detailsJson,
            Instant createdAt) {
        Instant normalizedTime = normalizeTimestamp(createdAt);
        StringBuilder sb = new StringBuilder(256);
        append(sb, prevHash);
        append(sb, eventId.toString());
        append(sb, actor);
        append(sb, actorRole);
        append(sb, action);
        append(sb, targetType);
        append(sb, targetId);
        append(sb, merkleRoot);
        append(sb, detailsJson);
        append(sb, normalizedTime.toString());
        return ExceptionFingerprinter.sha256Hex(sb.toString());
    }

    public String computeMerkleRoot(List<UUID> recordIds) {
        if (recordIds.isEmpty()) {
            return ExceptionFingerprinter.sha256Hex("dlt-deck-merkle-empty");
        }
        List<byte[]> level = new ArrayList<>(recordIds.size());
        for (UUID id : recordIds) {
            level.add(sha256Bytes(("leaf:" + id).getBytes(StandardCharsets.UTF_8)));
        }
        while (level.size() > 1) {
            List<byte[]> next = new ArrayList<>((level.size() + 1) / 2);
            for (int i = 0; i < level.size(); i += 2) {
                byte[] left = level.get(i);
                byte[] right = (i + 1 < level.size()) ? level.get(i + 1) : left;
                byte[] combined = new byte[1 + left.length + right.length];
                combined[0] = 0x01; // domain separation between leaf and interior nodes
                System.arraycopy(left, 0, combined, 1, left.length);
                System.arraycopy(right, 0, combined, 1 + left.length, right.length);
                next.add(sha256Bytes(combined));
            }
            level = next;
        }
        return LOWER_HEX.formatHex(level.getFirst());
    }

    public AuditChainVerification verifyChain(List<AuditEntry> orderedEntries) {
        String expectedPrev = GENESIS_HASH;
        long checked = 0;
        String latestHash = null;
        for (AuditEntry entry : orderedEntries) {
            checked++;
            if (!constantTimeEquals(expectedPrev, entry.prevHash())) {
                return AuditChainVerification.broken(
                        checked,
                        entry.sequenceNo(),
                        "Broken prevHash link at sequence " + entry.sequenceNo());
            }
            String recomputed = computeEntryHash(
                    entry.prevHash(),
                    entry.eventId(),
                    entry.actor(),
                    entry.actorRole(),
                    entry.action(),
                    entry.targetType(),
                    entry.targetId(),
                    entry.merkleRoot(),
                    entry.detailsJson(),
                    entry.createdAt());
            if (!constantTimeEquals(recomputed, entry.entryHash())) {
                return AuditChainVerification.broken(
                        checked,
                        entry.sequenceNo(),
                        "Tampered entryHash detected at sequence " + entry.sequenceNo());
            }
            expectedPrev = entry.entryHash();
            latestHash = entry.entryHash();
        }
        return AuditChainVerification.intact(checked, latestHash);
    }

    public static Instant normalizeTimestamp(Instant timestamp) {
        return timestamp.truncatedTo(ChronoUnit.MICROS);
    }

    private static void append(StringBuilder sb, @Nullable String value) {
        if (value == null) {
            sb.append("-1:|");
        } else {
            sb.append(value.length()).append(':').append(value).append('|');
        }
    }

    private static byte[] sha256Bytes(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }

    private static boolean constantTimeEquals(String a, String b) {
        return MessageDigest.isEqual(
                a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }
}
