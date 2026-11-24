package io.github.duhanguler.dltdeck.starter.security;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Base64;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Enforces strict per-request cryptographic 128-bit Nonce CSP (zero {@code 'unsafe-inline'}, zero external CDN),
 * anti-clickjacking, and {@code Cache-Control: no-store} exclusively on {@code dlt.deck.base-path} requests.
 */
public final class StrictCspNonceFilter extends OncePerRequestFilter {

    public static final String CSP_NONCE_ATTRIBUTE = "dltDeckCspNonce";
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private final String basePath;

    public StrictCspNonceFilter(String basePath) {
        this.basePath = basePath;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String uri = request.getRequestURI();
        return !uri.equals(basePath) && !uri.startsWith(basePath + "/");
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        byte[] nonceBytes = new byte[16];
        SECURE_RANDOM.nextBytes(nonceBytes);
        String nonce = Base64.getEncoder().encodeToString(nonceBytes);
        request.setAttribute(CSP_NONCE_ATTRIBUTE, nonce);

        String csp = "default-src 'self'; "
                + "script-src 'self' 'nonce-" + nonce + "'; "
                + "style-src 'self' 'nonce-" + nonce + "'; "
                + "img-src 'self' data:; "
                + "connect-src 'self'; "
                + "frame-ancestors 'none'; "
                + "base-uri 'self'; "
                + "form-action 'self'";

        response.setHeader("Content-Security-Policy", csp);
        response.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
        response.setHeader("Pragma", "no-cache");
        response.setHeader("X-Content-Type-Options", "nosniff");
        response.setHeader("X-Frame-Options", "DENY");
        response.setHeader("Referrer-Policy", "no-referrer");

        filterChain.doFilter(request, response);
    }
}
