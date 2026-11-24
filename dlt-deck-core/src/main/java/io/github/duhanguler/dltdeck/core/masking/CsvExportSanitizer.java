package io.github.duhanguler.dltdeck.core.masking;

import org.jspecify.annotations.Nullable;

/**
 * Neutralizes CSV formula injection (OWASP CSV Injection: cells starting with '=', '+', '-', '@', '\t', '\r')
 * and applies strict RFC 4180 quoting.
 */
public final class CsvExportSanitizer {

    private CsvExportSanitizer() {}

    public static String sanitizeCell(@Nullable String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        String neutralized = value;
        char first = neutralized.charAt(0);
        if (first == '=' || first == '+' || first == '-' || first == '@' || first == '\t' || first == '\r') {
            neutralized = "'" + neutralized;
        }
        boolean needsQuotes = neutralized.indexOf(',') >= 0
                || neutralized.indexOf('"') >= 0
                || neutralized.indexOf('\n') >= 0
                || neutralized.indexOf('\r') >= 0;
        if (!needsQuotes) {
            return neutralized;
        }
        return "\"" + neutralized.replace("\"", "\"\"") + "\"";
    }
}
