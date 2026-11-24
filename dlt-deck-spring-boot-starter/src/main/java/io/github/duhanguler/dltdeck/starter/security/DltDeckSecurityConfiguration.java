package io.github.duhanguler.dltdeck.starter.security;

import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.security.web.csrf.CookieCsrfTokenRepository;
import org.springframework.security.web.csrf.CsrfTokenRequestAttributeHandler;

/**
 * Isolated fail-closed {@link SecurityFilterChain} scoped strictly to {@code dlt.deck.base-path/**}
 * so the host application's own security rules are never modified or overridden.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(SecurityFilterChain.class)
@ConditionalOnBean(HttpSecurity.class)
@ConditionalOnProperty(prefix = "dlt.deck.security", name = "enabled", havingValue = "true", matchIfMissing = true)
public class DltDeckSecurityConfiguration {

    @Bean(name = "dltDeckSecurityFilterChain")
    @Order(Ordered.HIGHEST_PRECEDENCE + 20)
    @ConditionalOnMissingBean(name = "dltDeckSecurityFilterChain")
    public SecurityFilterChain dltDeckSecurityFilterChain(HttpSecurity http, DltDeckProperties properties)
            throws Exception {
        String base = properties.getBasePath();
        String viewer = roleOrDefault(properties.getSecurity().getViewerRole(), "DLT_VIEWER");
        String operator = roleOrDefault(properties.getSecurity().getOperatorRole(), "DLT_OPERATOR");
        String approver = roleOrDefault(properties.getSecurity().getApproverRole(), "DLT_APPROVER");

        http.securityMatcher(base, base + "/**")
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers(HttpMethod.POST, base + "/api/plans/*/approve", base + "/api/plans/*/reject")
                        .hasAnyRole(approver, "ADMIN")
                        .requestMatchers(HttpMethod.POST, base + "/api/messages/*/resolve")
                        .hasAnyRole(approver, "ADMIN")
                        .requestMatchers(HttpMethod.POST, base + "/api/**")
                        .hasAnyRole(operator, approver, "ADMIN")
                        .requestMatchers(HttpMethod.GET, base, base + "/", base + "/api/**")
                        .hasAnyRole(viewer, operator, approver, "ADMIN")
                        .anyRequest()
                        .denyAll())
                .httpBasic(Customizer.withDefaults())
                .addFilterBefore(new StrictCspNonceFilter(base), UsernamePasswordAuthenticationFilter.class)
                .addFilterAfter(new ActorContextScopedValueFilter(properties), AuthorizationFilter.class);

        if (properties.getSecurity().isCsrfEnabled()) {
            CookieCsrfTokenRepository csrfRepo = CookieCsrfTokenRepository.withHttpOnlyFalse();
            csrfRepo.setCookiePath(base);
            CsrfTokenRequestAttributeHandler handler = new CsrfTokenRequestAttributeHandler();
            handler.setCsrfRequestAttributeName(null);
            http.csrf(csrf -> csrf.csrfTokenRepository(csrfRepo).csrfTokenRequestHandler(handler));
        } else {
            http.csrf(csrf -> csrf.disable());
        }
        return http.build();
    }

    private static String roleOrDefault(@org.jspecify.annotations.Nullable String configured, String fallback) {
        if (configured == null || configured.isBlank()) {
            return fallback;
        }
        return configured.startsWith("ROLE_") ? configured.substring(5) : configured;
    }
}
