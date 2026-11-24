package io.github.duhanguler.dltdeck.core.masking;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Checksum-verified PII redactor for UI previews, MCP responses, exports, and audit details.
 * Raw Kafka bytes stored for replay are never mutated by this component.
 *
 * <p>Validates mathematical checksums before masking to eliminate false positives on order IDs and timestamps:
 * <ul>
 *   <li>Turkish TCKN (11-digit mod-10 / mod-11 checksum)</li>
 *   <li>IBAN (ISO 13616 mod-97 checksum)</li>
 *   <li>Payment Card PAN (13..19 digits passing Luhn checksum)</li>
 *   <li>Email addresses, phone numbers, JWT/Bearer tokens, and sensitive JSON/header keys</li>
 * </ul>
 */
public final class PiiRedactor {

    private static final Pattern JWT_PATTERN =
            Pattern.compile("(?i)(?:Bearer\\s+)?eyJ[A-Za-z0-9_-]{8,}\\.eyJ[A-Za-z0-9_-]{8,}\\.[A-Za-z0-9_-]{8,}");
    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("\\b([A-Za-z0-9._%+-]{1,64})@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})\\b");
    private static final Pattern IBAN_PATTERN =
            Pattern.compile("\\b([A-Z]{2}\\d{2}(?:[\\s-]?[A-Z0-9]){11,30})\\b");
    private static final Pattern CARD_PATTERN =
            Pattern.compile("(?<!\\d)(\\d{4}(?:[\\s-]?\\d{4}){2,3}(?:[\\s-]?\\d{1,3})?)(?!\\d)");
    private static final Pattern TCKN_PATTERN = Pattern.compile("(?<!\\d)([1-9]\\d{10})(?!\\d)");
    private static final Pattern PHONE_PATTERN =
            Pattern.compile("(?<!\\d)(\\+\\d{1,3}[\\s-]?\\(?\\d{3}\\)?[\\s-]?\\d{3}[\\s-]?\\d{2}[\\s-]?\\d{2})(?!\\d)");
    private static final Pattern SENSITIVE_JSON_FIELD_PATTERN = Pattern.compile(
            "(?i)(\"(?:password|passwd|secret|token|accessToken|refreshToken|apiKey|api_key|authorization|cvv|cvc|pin|ssn|tckn|tcKimlikNo|nationalId|iban|cardNumber|pan|creditCard)\"\\s*:\\s*\")([^\"]*)(\")");

    private static final Set<String> SENSITIVE_HEADER_KEYS = Set.of(
            "authorization",
            "proxy-authorization",
            "cookie",
            "set-cookie",
            "x-api-key",
            "x-auth-token",
            "x-csrf-token",
            "x-xsrf-token",
            "api-key",
            "token",
            "secret",
            "password");

    public String redactText(String input) {
        if (input.isEmpty()) {
            return input;
        }
        String result = SENSITIVE_JSON_FIELD_PATTERN.matcher(input).replaceAll("$1[REDACTED_SECRET]$3");
        result = JWT_PATTERN.matcher(result).replaceAll("[REDACTED_JWT]");
        result = replaceIfValid(IBAN_PATTERN, result, PiiRedactor::isValidIban, "[REDACTED_IBAN]");
        result = replaceIfValid(CARD_PATTERN, result, PiiRedactor::isValidLuhnPan, "[REDACTED_PAN]");
        result = replaceIfValid(TCKN_PATTERN, result, PiiRedactor::isValidTckn, "[REDACTED_TCKN]");
        result = EMAIL_PATTERN.matcher(result).replaceAll(mr -> maskEmail(mr.group(1), mr.group(2)));
        result = PHONE_PATTERN.matcher(result).replaceAll("[REDACTED_PHONE]");
        return result;
    }

    public Map<String, String> redactHeaders(Map<String, String> headers) {
        Map<String, String> out = new LinkedHashMap<>(headers.size());
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            String key = entry.getKey();
            String lowerKey = key.toLowerCase(Locale.ROOT);
            if (SENSITIVE_HEADER_KEYS.contains(lowerKey)
                    || lowerKey.contains("secret")
                    || lowerKey.contains("token")
                    || lowerKey.contains("password")
                    || lowerKey.contains("authorization")) {
                out.put(key, "[REDACTED_HEADER]");
            } else {
                out.put(key, redactText(entry.getValue()));
            }
        }
        return out;
    }

    /**
     * Validates a Turkish Citizenship Number (TCKN) using official mod-10 and mod-11 checksums.
     */
    public static boolean isValidTckn(String raw) {
        if (raw.length() != 11 || raw.charAt(0) == '0') {
            return false;
        }
        int[] d = new int[11];
        for (int i = 0; i < 11; i++) {
            char c = raw.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
            d[i] = c - '0';
        }
        int oddSum = d[0] + d[2] + d[4] + d[6] + d[8];
        int evenSum = d[1] + d[3] + d[5] + d[7];
        int d10 = Math.floorMod(oddSum * 7 - evenSum, 10);
        if (d[9] != d10) {
            return false;
        }
        int firstTenSum = 0;
        for (int i = 0; i < 10; i++) {
            firstTenSum += d[i];
        }
        return d[10] == (firstTenSum % 10);
    }

    /**
     * Validates an IBAN according to ISO 13616 (rearrange first 4 chars to end, letters -> 10..35, mod 97 == 1).
     */
    public static boolean isValidIban(String raw) {
        String compact = raw.replaceAll("[\\s-]+", "").toUpperCase(Locale.ROOT);
        if (compact.length() < 15 || compact.length() > 34) {
            return false;
        }
        if (!Character.isLetter(compact.charAt(0))
                || !Character.isLetter(compact.charAt(1))
                || !Character.isDigit(compact.charAt(2))
                || !Character.isDigit(compact.charAt(3))) {
            return false;
        }
        String rearranged = compact.substring(4) + compact.substring(0, 4);
        StringBuilder numeric = new StringBuilder(rearranged.length() * 2);
        for (int i = 0; i < rearranged.length(); i++) {
            char c = rearranged.charAt(i);
            if (c >= '0' && c <= '9') {
                numeric.append(c);
            } else if (c >= 'A' && c <= 'Z') {
                numeric.append(c - 'A' + 10);
            } else {
                return false;
            }
        }
        return new BigInteger(numeric.toString()).mod(BigInteger.valueOf(97)).intValue() == 1;
    }

    /**
     * Validates a payment card Primary Account Number (13..19 digits) via the Luhn algorithm.
     */
    public static boolean isValidLuhnPan(String raw) {
        String digits = raw.replaceAll("[\\s-]+", "");
        if (digits.length() < 13 || digits.length() > 19) {
            return false;
        }
        int sum = 0;
        boolean doubleDigit = false;
        for (int i = digits.length() - 1; i >= 0; i--) {
            char c = digits.charAt(i);
            if (c < '0' || c > '9') {
                return false;
            }
            int n = c - '0';
            if (doubleDigit) {
                n *= 2;
                if (n > 9) {
                    n -= 9;
                }
            }
            sum += n;
            doubleDigit = !doubleDigit;
        }
        return sum % 10 == 0;
    }

    private static String maskEmail(String local, String domain) {
        if (local.length() <= 1) {
            return "*@" + domain;
        }
        return local.charAt(0) + "***@" + domain;
    }

    @FunctionalInterface
    private interface MatchValidator {
        boolean isValid(String candidate);
    }

    private static String replaceIfValid(
            Pattern pattern, String input, MatchValidator validator, String replacement) {
        Matcher matcher = pattern.matcher(input);
        StringBuilder sb = new StringBuilder(input.length());
        while (matcher.find()) {
            String candidate = matcher.group(1);
            if (validator.isValid(candidate)) {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
            } else {
                matcher.appendReplacement(sb, Matcher.quoteReplacement(matcher.group(0)));
            }
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
}
