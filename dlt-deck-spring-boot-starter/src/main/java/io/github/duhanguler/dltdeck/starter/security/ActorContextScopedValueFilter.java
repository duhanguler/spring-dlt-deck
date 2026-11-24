package io.github.duhanguler.dltdeck.starter.security;

import io.github.duhanguler.dltdeck.core.context.ActorContext;
import io.github.duhanguler.dltdeck.core.context.ActorRole;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collection;
import java.util.UUID;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Binds the authenticated Spring Security principal into Java 25 final {@link ScopedValue}
 * ({@link ActorContext#CURRENT}) for the duration of every {@code spring-dlt-deck} HTTP request.
 */
public final class ActorContextScopedValueFilter extends OncePerRequestFilter {

    private final DltDeckProperties properties;

    public ActorContextScopedValueFilter(DltDeckProperties properties) {
        this.properties = properties;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String basePath = properties.getBasePath();
        String uri = request.getRequestURI();
        return !uri.equals(basePath) && !uri.startsWith(basePath + "/");
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        ActorContext actor = resolveActor(request);
        try {
            ScopedValue.where(ActorContext.CURRENT, actor).call(() -> {
                filterChain.doFilter(request, response);
                return null;
            });
        } catch (ServletException | IOException | RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ServletException("ScopedValue execution failure", ex);
        }
    }

    private ActorContext resolveActor(HttpServletRequest request) {
        String corrId = request.getHeader("X-Correlation-Id");
        if (corrId == null || corrId.isBlank()) {
            corrId = UUID.randomUUID().toString();
        }
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        if (auth == null || !auth.isAuthenticated() || "anonymousUser".equals(auth.getName())) {
            return new ActorContext("anonymous", ActorRole.VIEWER, corrId);
        }
        ActorRole role = mapRole(auth.getAuthorities());
        return new ActorContext(auth.getName(), role, corrId);
    }

    private ActorRole mapRole(Collection<? extends GrantedAuthority> authorities) {
        String approverRole = normalizeRole(properties.getSecurity().getApproverRole(), "DLT_APPROVER");
        String operatorRole = normalizeRole(properties.getSecurity().getOperatorRole(), "DLT_OPERATOR");

        boolean hasApprover = false;
        boolean hasOperator = false;
        for (GrantedAuthority ga : authorities) {
            String name = ga.getAuthority();
            if (name == null) {
                continue;
            }
            if (name.equals(approverRole) || name.equals("ROLE_" + approverRole) || name.equals("ROLE_ADMIN")) {
                hasApprover = true;
            } else if (name.equals(operatorRole) || name.equals("ROLE_" + operatorRole)) {
                hasOperator = true;
            }
        }
        if (hasApprover) {
            return ActorRole.APPROVER;
        }
        if (hasOperator) {
            return ActorRole.OPERATOR;
        }
        return ActorRole.VIEWER;
    }

    private static String normalizeRole(@org.jspecify.annotations.Nullable String configured, String fallback) {
        return (configured == null || configured.isBlank()) ? fallback : configured.trim();
    }
}
