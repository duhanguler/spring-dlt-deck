package io.github.duhanguler.dltdeck.core.fingerprint;

import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * Deterministic SHA-256 exception fingerprinter & failure classifier.
 * Normalizes volatile tokens (UUIDs, timestamps, memory addresses, numeric IDs, CGLIB/Lambda proxy suffixes)
 * so incident storms collapse cleanly into a single deduplicated fingerprint cluster.
 */
public final class ExceptionFingerprinter {

    private static final int DEFAULT_TOP_FRAMES = 5;
    private static final int MAX_CANONICAL_STACK_CHARS = 8192;

    private static final Pattern UUID_PATTERN = Pattern.compile(
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}");
    private static final Pattern ISO_TIMESTAMP_PATTERN = Pattern.compile(
            "\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:?\\d{2})?");
    private static final Pattern HEX_ADDRESS_PATTERN = Pattern.compile("(?:0x[0-9a-fA-F]+|@[0-9a-fA-F]{4,})");
    private static final Pattern LONG_NUMBER_PATTERN = Pattern.compile("\\b\\d{3,}\\b");
    private static final Pattern PROXY_SUFFIX_PATTERN = Pattern.compile(
            "\\$\\$(?:SpringCGLIB|EnhancerBySpringCGLIB|FastClassBySpringCGLIB|Lambda)[^\\s:(]*");
    private static final Pattern LINE_NUMBER_PATTERN = Pattern.compile(":\\d+\\)");
    private static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
    private static final HexFormat LOWER_HEX = HexFormat.of();

    private final int maxFrames;

    public ExceptionFingerprinter() {
        this(DEFAULT_TOP_FRAMES);
    }

    public ExceptionFingerprinter(int maxFrames) {
        if (maxFrames <= 0) {
            throw new IllegalArgumentException("maxFrames must be positive");
        }
        this.maxFrames = maxFrames;
    }

    public FingerprintResult fingerprint(
            @Nullable String exceptionFqcn,
            @Nullable String rootCauseFqcn,
            @Nullable String exceptionMessage,
            @Nullable String stacktrace) {
        String effectiveException = sanitizeTypeName(exceptionFqcn);
        String effectiveRootCause = sanitizeTypeName(rootCauseFqcn != null && !rootCauseFqcn.isBlank()
                ? rootCauseFqcn
                : extractRootCauseFqcn(stacktrace, effectiveException));
        String normalizedMessage = normalizeDynamicTokens(exceptionMessage == null ? "" : exceptionMessage);
        List<String> normalizedFrames = extractTopNormalizedFrames(stacktrace);

        String canonicalInput = effectiveRootCause
                + "|" + effectiveException
                + "|" + normalizedMessage
                + "|" + String.join(";", normalizedFrames);

        String hash = sha256Hex(canonicalInput);
        FailureCategory category = classify(effectiveException, effectiveRootCause, exceptionMessage);
        String canonicalStack = canonicalizeStacktrace(stacktrace);
        String sampleMsg = exceptionMessage == null ? null : truncate(exceptionMessage.trim(), 1024);

        return new FingerprintResult(
                hash,
                effectiveException.isEmpty() ? null : effectiveException,
                effectiveRootCause.isEmpty() ? null : effectiveRootCause,
                sampleMsg,
                canonicalStack,
                category,
                category.defaultRecommendation());
    }

    public String normalizeDynamicTokens(String input) {
        if (input.isEmpty()) {
            return "";
        }
        String s = UUID_PATTERN.matcher(input).replaceAll("<UUID>");
        s = ISO_TIMESTAMP_PATTERN.matcher(s).replaceAll("<TS>");
        s = HEX_ADDRESS_PATTERN.matcher(s).replaceAll("<HEX>");
        s = PROXY_SUFFIX_PATTERN.matcher(s).replaceAll("<PROXY>");
        s = LONG_NUMBER_PATTERN.matcher(s).replaceAll("<NUM>");
        s = WHITESPACE_PATTERN.matcher(s.trim()).replaceAll(" ");
        return truncate(s, 512);
    }

    private List<String> extractTopNormalizedFrames(@Nullable String stacktrace) {
        if (stacktrace == null || stacktrace.isBlank()) {
            return List.of();
        }
        List<String> appFrames = new ArrayList<>();
        List<String> fallbackFrames = new ArrayList<>();

        for (String rawLine : stacktrace.lines().toList()) {
            String line = rawLine.trim();
            if (!line.startsWith("at ")) {
                continue;
            }
            String cleaned = PROXY_SUFFIX_PATTERN.matcher(line).replaceAll("");
            cleaned = LINE_NUMBER_PATTERN.matcher(cleaned).replaceAll(":<LINE>)");
            if (isFrameworkNoise(cleaned)) {
                if (fallbackFrames.size() < maxFrames) {
                    fallbackFrames.add(cleaned);
                }
            } else {
                appFrames.add(cleaned);
                if (appFrames.size() >= maxFrames) {
                    break;
                }
            }
        }
        return appFrames.isEmpty() ? fallbackFrames : appFrames;
    }

    private static boolean isFrameworkNoise(String frame) {
        return frame.startsWith("at java.base/")
                || frame.startsWith("at jdk.internal.")
                || frame.startsWith("at sun.reflect.")
                || frame.startsWith("at org.springframework.cglib.")
                || frame.startsWith("at org.springframework.aop.")
                || frame.startsWith("at org.springframework.kafka.listener.");
    }

    private static String extractRootCauseFqcn(@Nullable String stacktrace, String fallback) {
        if (stacktrace == null || stacktrace.isBlank()) {
            return fallback;
        }
        String lastCause = fallback;
        for (String rawLine : stacktrace.lines().toList()) {
            String line = rawLine.trim();
            if (line.startsWith("Caused by:")) {
                String rest = line.substring("Caused by:".length()).trim();
                int colon = rest.indexOf(':');
                String candidate = colon > 0 ? rest.substring(0, colon).trim() : rest;
                if (!candidate.isEmpty()) {
                    lastCause = sanitizeTypeName(candidate);
                }
            }
        }
        return lastCause;
    }

    private static FailureCategory classify(
            String exceptionFqcn, String rootCauseFqcn, @Nullable String message) {
        String combined = (exceptionFqcn + " " + rootCauseFqcn + " " + (message == null ? "" : message))
                .toLowerCase(Locale.ROOT);

        if (combined.contains("deserializationexception")
                || combined.contains("jsonparseexception")
                || combined.contains("streamreadexception")
                || combined.contains("mismatchedinputexception")
                || combined.contains("serializationexception")
                || combined.contains("invalidformatexception")
                || combined.contains("eofexception")) {
            return FailureCategory.DESERIALIZATION;
        }
        if (combined.contains("schemaregistry")
                || combined.contains("incompatible schema")
                || combined.contains("avrotypeexception")
                || combined.contains("unknown magic byte")
                || combined.contains("invalidprotocolbufferexception")) {
            return FailureCategory.SCHEMA_MISMATCH;
        }
        if (combined.contains("constraintviolation")
                || combined.contains("validationexception")
                || combined.contains("methodargumentnotvalid")
                || combined.contains("illegalargumentexception")) {
            return FailureCategory.VALIDATION;
        }
        if (combined.contains("timeout")
                || combined.contains("connectexception")
                || combined.contains("socketexception")
                || combined.contains("serviceunavailable")
                || combined.contains("503")
                || combined.contains("504")
                || combined.contains("429")
                || combined.contains("deadlock")
                || combined.contains("transientdataaccess")) {
            return FailureCategory.TRANSIENT_DOWNSTREAM;
        }
        if (combined.contains("accessdenied")
                || combined.contains("authorizationexception")
                || combined.contains("authentication")
                || combined.contains("unauthorized")
                || combined.contains("forbidden")
                || combined.contains("401")
                || combined.contains("403")) {
            return FailureCategory.AUTH_PERMISSION;
        }
        if (combined.contains("nullpointer")
                || combined.contains("illegalstate")
                || combined.contains("numberformat")
                || combined.contains("arithmeticexception")
                || combined.contains("dataintegrityviolation")) {
            return FailureCategory.POISON_DATA;
        }
        return FailureCategory.UNKNOWN;
    }

    private static String sanitizeTypeName(@Nullable String fqcn) {
        if (fqcn == null || fqcn.isBlank()) {
            return "";
        }
        return PROXY_SUFFIX_PATTERN.matcher(fqcn.trim()).replaceAll("");
    }

    private static @Nullable String canonicalizeStacktrace(@Nullable String stacktrace) {
        if (stacktrace == null || stacktrace.isBlank()) {
            return null;
        }
        String normalized = PROXY_SUFFIX_PATTERN.matcher(stacktrace.trim()).replaceAll("<PROXY>");
        return truncate(normalized, MAX_CANONICAL_STACK_CHARS);
    }

    private static String truncate(String value, int maxLen) {
        return value.length() <= maxLen ? value : value.substring(0, maxLen);
    }

    public static String sha256Hex(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return LOWER_HEX.formatHex(digest);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 unavailable", ex);
        }
    }

    public record FingerprintResult(
            String fingerprint,
            @Nullable String exceptionFqcn,
            @Nullable String rootCauseFqcn,
            @Nullable String sampleMessage,
            @Nullable String canonicalStacktrace,
            FailureCategory failureCategory,
            String recommendation) {}
}
