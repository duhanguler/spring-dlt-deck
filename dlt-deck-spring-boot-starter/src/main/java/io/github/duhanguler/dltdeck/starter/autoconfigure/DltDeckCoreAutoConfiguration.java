package io.github.duhanguler.dltdeck.starter.autoconfigure;

import io.github.duhanguler.dltdeck.core.codec.DefaultPayloadCodec;
import io.github.duhanguler.dltdeck.core.codec.PayloadCodec;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.patch.JsonPayloadPatcher;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.BlastRadiusGuard;
import io.github.duhanguler.dltdeck.core.policy.KeysetCursorCodec;
import io.github.duhanguler.dltdeck.core.policy.MakerCheckerPolicy;
import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import io.github.duhanguler.dltdeck.core.spi.ReplayRateLimitCoordinator;
import io.github.duhanguler.dltdeck.starter.aot.DltDeckRuntimeHints;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import io.github.duhanguler.dltdeck.starter.service.LocalRateLimitCoordinator;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ImportRuntimeHints;
import tools.jackson.databind.json.JsonMapper;

@AutoConfiguration
@EnableConfigurationProperties(DltDeckProperties.class)
@ConditionalOnProperty(prefix = "dlt.deck", name = "enabled", havingValue = "true", matchIfMissing = true)
@ImportRuntimeHints(DltDeckRuntimeHints.class)
public class DltDeckCoreAutoConfiguration {

    /**
     * Private Jackson 3 {@link JsonMapper} qualified strictly for {@code spring-dlt-deck} internals;
     * never overrides or mutates the host application's {@code ObjectMapper}.
     */
    @Bean("dltDeckJsonMapper")
    @Qualifier("dltDeckInternal")
    @ConditionalOnMissingBean(name = "dltDeckJsonMapper")
    public JsonMapper dltDeckJsonMapper() {
        return JsonMapper.builder().build();
    }

    @Bean("dltDeckClock")
    @Qualifier("dltDeckInternal")
    @ConditionalOnMissingBean(name = "dltDeckClock")
    public Clock dltDeckClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean
    public PayloadCodec dltDeckPayloadCodec(@Qualifier("dltDeckInternal") JsonMapper jsonMapper) {
        return new DefaultPayloadCodec(jsonMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public ExceptionFingerprinter dltDeckExceptionFingerprinter() {
        return new ExceptionFingerprinter();
    }

    @Bean
    @ConditionalOnMissingBean
    public PiiRedactor dltDeckPiiRedactor() {
        return new PiiRedactor();
    }

    @Bean
    @ConditionalOnMissingBean
    public JsonPayloadPatcher dltDeckJsonPayloadPatcher(
            @Qualifier("dltDeckInternal") JsonMapper jsonMapper, DltDeckProperties properties) {
        return new JsonPayloadPatcher(
                jsonMapper,
                properties.getStorage().getMaxPayloadBytes(),
                JsonPayloadPatcher.DEFAULT_MAX_NESTING_DEPTH);
    }

    @Bean
    @ConditionalOnMissingBean
    public ReplayHeaderSanitizer dltDeckReplayHeaderSanitizer(@Qualifier("dltDeckInternal") Clock clock) {
        return new ReplayHeaderSanitizer(clock);
    }

    @Bean
    @ConditionalOnMissingBean
    public BlastRadiusGuard dltDeckBlastRadiusGuard(
            @Qualifier("dltDeckInternal") Clock clock, DltDeckProperties properties) {
        var r = properties.getReplay();
        return new BlastRadiusGuard(
                clock,
                r.getBlastRadiusWindowSize(),
                r.getBlastRadiusMaxReDltRatio(),
                r.getBlastRadiusRetentionWindow());
    }

    @Bean
    @ConditionalOnMissingBean
    public MakerCheckerPolicy dltDeckMakerCheckerPolicy() {
        return new MakerCheckerPolicy();
    }

    @Bean
    @ConditionalOnMissingBean
    public AuditHashChain dltDeckAuditHashChain() {
        return new AuditHashChain();
    }

    @Bean
    @ConditionalOnMissingBean
    public KeysetCursorCodec dltDeckKeysetCursorCodec(DltDeckProperties properties) {
        return new KeysetCursorCodec(
                properties.getStorage().getCursorHmacSecret().getBytes(StandardCharsets.UTF_8));
    }

    @Bean
    @ConditionalOnMissingBean(ReplayRateLimitCoordinator.class)
    public ReplayRateLimitCoordinator dltDeckLocalRateLimitCoordinator(@Qualifier("dltDeckInternal") Clock clock) {
        return new LocalRateLimitCoordinator(clock);
    }
}
