package io.github.duhanguler.dltdeck.starter.autoconfigure;

import io.github.duhanguler.dltdeck.core.codec.PayloadCodec;
import io.github.duhanguler.dltdeck.core.fingerprint.ExceptionFingerprinter;
import io.github.duhanguler.dltdeck.core.masking.PiiRedactor;
import io.github.duhanguler.dltdeck.core.patch.JsonPayloadPatcher;
import io.github.duhanguler.dltdeck.core.policy.AuditHashChain;
import io.github.duhanguler.dltdeck.core.policy.BlastRadiusGuard;
import io.github.duhanguler.dltdeck.core.policy.MakerCheckerPolicy;
import io.github.duhanguler.dltdeck.core.policy.ReplayHeaderSanitizer;
import io.github.duhanguler.dltdeck.core.spi.AuditLedgerRepository;
import io.github.duhanguler.dltdeck.core.spi.DltDryRunHandler;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import io.github.duhanguler.dltdeck.core.spi.ReplayPlanRepository;
import io.github.duhanguler.dltdeck.core.spi.ReplayRateLimitCoordinator;
import io.github.duhanguler.dltdeck.core.spi.StripeLeaseStore;
import io.github.duhanguler.dltdeck.starter.config.DltDeckProperties;
import io.github.duhanguler.dltdeck.starter.kafka.DltBatchIngestionEngine;
import io.github.duhanguler.dltdeck.starter.kafka.DltDeckIdempotentReplayFilter;
import io.github.duhanguler.dltdeck.starter.kafka.KeyStripedReplayExecutor;
import io.github.duhanguler.dltdeck.starter.kafka.SafeSpringHeaderExtractor;
import io.github.duhanguler.dltdeck.starter.kafka.TargetTopicValidator;
import io.github.duhanguler.dltdeck.starter.kafka.TieredDryRunService;
import io.github.duhanguler.dltdeck.starter.service.DltDeckControlPlaneService;
import jakarta.validation.Validator;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import tools.jackson.databind.json.JsonMapper;

/**
 * Isolated Kafka 4.4 beans qualified strictly under {@code @Qualifier("dltDeckInternal")}.
 * Never injects into or overrides the host application's {@link KafkaTemplate}, {@link ProducerFactory},
 * or {@code ConsumerFactory}.
 */
@AutoConfiguration(after = {DltDeckCoreAutoConfiguration.class, DltDeckJdbcStorageAutoConfiguration.class})
@ConditionalOnProperty(prefix = "dlt.deck", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnBean({DltMessageRepository.class, ReplayPlanRepository.class, StripeLeaseStore.class, AuditLedgerRepository.class})
public class DltDeckKafkaAutoConfiguration {

    @Bean(name = "dltDeckInternalAdminClient", destroyMethod = "close")
    @Qualifier("dltDeckInternal")
    @ConditionalOnMissingBean(name = "dltDeckInternalAdminClient")
    public AdminClient dltDeckInternalAdminClient(Environment env) {
        Map<String, Object> props = baseKafkaProps(env);
        props.put(CommonClientConfigs.CLIENT_ID_CONFIG, "spring-dlt-deck-admin");
        return AdminClient.create(props);
    }

    @Bean(name = "dltDeckInternalProducerFactory")
    @Qualifier("dltDeckInternal")
    @ConditionalOnMissingBean(name = "dltDeckInternalProducerFactory")
    public ProducerFactory<byte[], byte[]> dltDeckInternalProducerFactory(Environment env) {
        Map<String, Object> props = baseKafkaProps(env);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "spring-dlt-deck-replay-producer");
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(name = "dltDeckInternalKafkaTemplate")
    @Qualifier("dltDeckInternal")
    @ConditionalOnMissingBean(name = "dltDeckInternalKafkaTemplate")
    public KafkaTemplate<byte[], byte[]> dltDeckInternalKafkaTemplate(
            @Qualifier("dltDeckInternal") ProducerFactory<byte[], byte[]> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    @Bean
    @ConditionalOnMissingBean
    public SafeSpringHeaderExtractor dltDeckSafeSpringHeaderExtractor() {
        return new SafeSpringHeaderExtractor();
    }

    @Bean
    @ConditionalOnMissingBean
    public TargetTopicValidator dltDeckTargetTopicValidator(
            @Qualifier("dltDeckInternal") AdminClient adminClient, DltDeckProperties properties) {
        return new TargetTopicValidator(adminClient, properties.getReplay().isValidateTargetTopicExists());
    }

    @Bean
    @ConditionalOnMissingBean
    public DltBatchIngestionEngine dltDeckBatchIngestionEngine(
            Environment env,
            @Qualifier("dltDeckInternal") AdminClient adminClient,
            DltDeckProperties properties,
            SafeSpringHeaderExtractor headerExtractor,
            PayloadCodec payloadCodec,
            ExceptionFingerprinter fingerprinter,
            DltMessageRepository messageRepository,
            ReplayPlanRepository planRepository,
            StripeLeaseStore stripeLeaseStore,
            BlastRadiusGuard blastRadiusGuard,
            @Qualifier("dltDeckInternal") Clock clock) {
        return new DltBatchIngestionEngine(
                baseKafkaProps(env),
                adminClient,
                properties,
                headerExtractor,
                payloadCodec,
                fingerprinter,
                messageRepository,
                planRepository,
                stripeLeaseStore,
                blastRadiusGuard,
                clock);
    }

    @Bean
    @ConditionalOnMissingBean
    public KeyStripedReplayExecutor dltDeckKeyStripedReplayExecutor(
            @Qualifier("dltDeckInternal") KafkaTemplate<byte[], byte[]> kafkaTemplate,
            DltMessageRepository messageRepository,
            ReplayPlanRepository planRepository,
            StripeLeaseStore stripeLeaseStore,
            AuditLedgerRepository auditRepository,
            ReplayRateLimitCoordinator rateLimitCoordinator,
            TargetTopicValidator targetTopicValidator,
            JsonPayloadPatcher payloadPatcher,
            ReplayHeaderSanitizer headerSanitizer,
            MakerCheckerPolicy makerCheckerPolicy,
            BlastRadiusGuard blastRadiusGuard,
            AuditHashChain auditHashChain,
            DltDeckProperties properties,
            @Qualifier("dltDeckInternal") Clock clock) {
        return new KeyStripedReplayExecutor(
                kafkaTemplate,
                messageRepository,
                planRepository,
                stripeLeaseStore,
                auditRepository,
                rateLimitCoordinator,
                targetTopicValidator,
                payloadPatcher,
                headerSanitizer,
                makerCheckerPolicy,
                blastRadiusGuard,
                auditHashChain,
                properties,
                clock);
    }

    @Bean
    @ConditionalOnMissingBean
    public TieredDryRunService dltDeckTieredDryRunService(
            DltMessageRepository messageRepository,
            JsonPayloadPatcher payloadPatcher,
            PayloadCodec payloadCodec,
            @Qualifier("dltDeckInternal") JsonMapper jsonMapper,
            ObjectProvider<Validator> validatorProvider,
            ObjectProvider<DltDryRunHandler> handlerProvider,
            TargetTopicValidator targetTopicValidator,
            ReplayHeaderSanitizer headerSanitizer,
            @Qualifier("dltDeckInternal") KafkaTemplate<byte[], byte[]> kafkaTemplate) {
        List<DltDryRunHandler> handlers = handlerProvider.orderedStream().toList();
        return new TieredDryRunService(
                messageRepository,
                payloadPatcher,
                payloadCodec,
                jsonMapper,
                validatorProvider.getIfAvailable(),
                handlers,
                targetTopicValidator,
                headerSanitizer,
                kafkaTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public DltDeckIdempotentReplayFilter dltDeckIdempotentReplayFilter() {
        return new DltDeckIdempotentReplayFilter();
    }

    @Bean
    @ConditionalOnMissingBean
    public DltDeckControlPlaneService dltDeckControlPlaneService(
            DltMessageRepository messageRepository,
            ReplayPlanRepository planRepository,
            AuditLedgerRepository auditRepository,
            DltBatchIngestionEngine ingestionEngine,
            KeyStripedReplayExecutor replayExecutor,
            TieredDryRunService dryRunService,
            TargetTopicValidator targetTopicValidator,
            PayloadCodec payloadCodec,
            PiiRedactor piiRedactor,
            JsonPayloadPatcher payloadPatcher,
            MakerCheckerPolicy makerCheckerPolicy,
            DltDeckProperties properties,
            @Qualifier("dltDeckInternal") JsonMapper jsonMapper,
            @Qualifier("dltDeckInternal") Clock clock) {
        return new DltDeckControlPlaneService(
                messageRepository,
                planRepository,
                auditRepository,
                ingestionEngine,
                replayExecutor,
                dryRunService,
                targetTopicValidator,
                payloadCodec,
                piiRedactor,
                payloadPatcher,
                makerCheckerPolicy,
                properties,
                jsonMapper,
                clock);
    }

    private static Map<String, Object> baseKafkaProps(Environment env) {
        Map<String, Object> props = new LinkedHashMap<>();
        String bootstrap = env.getProperty(
                "dlt.deck.kafka.bootstrap-servers",
                env.getProperty("spring.kafka.bootstrap-servers", "localhost:9092"));
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        String securityProtocol = env.getProperty("dlt.deck.kafka.security-protocol");
        if (securityProtocol == null || securityProtocol.isBlank()) {
            securityProtocol = env.getProperty("spring.kafka.security.protocol");
        }
        if (securityProtocol != null && !securityProtocol.isBlank()) {
            props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
        }
        return props;
    }
}
