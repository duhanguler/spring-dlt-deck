package io.github.duhanguler.dltdeck.starter.autoconfigure;

import static org.springframework.web.servlet.function.RequestPredicates.GET;
import static org.springframework.web.servlet.function.RequestPredicates.POST;
import static org.springframework.web.servlet.function.RequestPredicates.path;
import static org.springframework.web.servlet.function.RouterFunctions.nest;
import static org.springframework.web.servlet.function.RouterFunctions.route;

import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import io.github.duhanguler.dltdeck.starter.security.StrictCspNonceFilter;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import io.github.duhanguler.dltdeck.starter.web.DltDeckWebHandler;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.function.RouterFunction;
import org.springframework.web.servlet.function.ServerResponse;
import tools.jackson.databind.json.JsonMapper;

/**
 * Isolated WebMvc.fn {@link RouterFunction} auto-configuration.
 * Strictly NEVER registers a global {@code WebMvcConfigurer} so host controllers, API versioning,
 * CORS, and message converters remain 100% untouched.
 */
@AutoConfiguration(after = DltDeckKafkaAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "dlt.deck", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnBean(DltDeckControlPlaneService.class)
public class DltDeckWebAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public DltDeckWebHandler dltDeckWebHandler(
            DltDeckControlPlaneService service,
            DltDeckProperties properties,
            @Qualifier("dltDeckInternal") JsonMapper jsonMapper) {
        return new DltDeckWebHandler(service, properties, jsonMapper);
    }

    @Bean(name = "dltDeckRouterFunction")
    @ConditionalOnMissingBean(name = "dltDeckRouterFunction")
    public RouterFunction<ServerResponse> dltDeckRouterFunction(
            DltDeckWebHandler handler, DltDeckProperties properties) {
        String base = properties.getBasePath();
        return nest(
                path(base),
                route(GET(""), handler::serveUi)
                        .andRoute(GET("/"), handler::serveUi)
                        .andRoute(GET("/api/overview"), handler::overview)
                        .andRoute(GET("/api/stream"), handler::sseStream)
                        .andRoute(GET("/api/clusters"), handler::listClusters)
                        .andRoute(GET("/api/messages"), handler::listMessages)
                        .andRoute(GET("/api/messages/export.csv"), handler::exportCsv)
                        .andRoute(GET("/api/messages/{id}"), handler::inspectMessage)
                        .andRoute(POST("/api/messages/{id}/patch-preview"), handler::previewPatch)
                        .andRoute(POST("/api/messages/{id}/dry-run"), handler::dryRun)
                        .andRoute(POST("/api/messages/{id}/discard"), handler::discardMessage)
                        .andRoute(POST("/api/messages/{id}/resolve"), handler::resolveIndeterminate)
                        .andRoute(GET("/api/plans"), handler::listPlans)
                        .andRoute(POST("/api/plans"), handler::draftPlan)
                        .andRoute(GET("/api/plans/{id}"), handler::getPlan)
                        .andRoute(POST("/api/plans/{id}/approve"), handler::approvePlan)
                        .andRoute(POST("/api/plans/{id}/reject"), handler::rejectPlan)
                        .andRoute(GET("/api/audit"), handler::listAudit)
                        .andRoute(GET("/api/audit/verify"), handler::verifyAudit));
    }

    @Bean(name = "dltDeckStandaloneCspFilter")
    @ConditionalOnMissingBean(name = "dltDeckSecurityFilterChain")
    public FilterRegistrationBean<StrictCspNonceFilter> dltDeckStandaloneCspFilter(DltDeckProperties properties) {
        FilterRegistrationBean<StrictCspNonceFilter> reg =
                new FilterRegistrationBean<>(new StrictCspNonceFilter(properties.getBasePath()));
        reg.addUrlPatterns(properties.getBasePath(), properties.getBasePath() + "/*");
        return reg;
    }
}
