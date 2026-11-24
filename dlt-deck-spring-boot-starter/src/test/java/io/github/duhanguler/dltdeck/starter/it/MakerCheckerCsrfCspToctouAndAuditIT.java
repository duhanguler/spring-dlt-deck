package io.github.duhanguler.dltdeck.starter.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.github.duhanguler.dltdeck.core.model.FailureCategory;
import io.github.duhanguler.dltdeck.core.model.IngestedDltMessage;
import io.github.duhanguler.dltdeck.core.model.MessageStatus;
import io.github.duhanguler.dltdeck.core.model.PayloadFormat;
import io.github.duhanguler.dltdeck.core.spi.DltMessageRepository;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * End-to-end security & governance IT verifying:
 * Strict Nonce CSP, {@code Cache-Control: no-store}, CSRF enforcement ({@code X-XSRF-TOKEN}),
 * four-eyes self-approval {@code 403 ProblemDetail}, TOCTOU {@code planSpecHash} tampering {@code 409 ProblemDetail},
 * independent approval, replay execution, and cryptographic {@code /api/audit/verify} integrity.
 */
@SpringBootTest(classes = HostAppNonInterferenceIT.HostTestApp.class)
class MakerCheckerCsrfCspToctouAndAuditIT {

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", SharedContainers.POSTGRES::getJdbcUrl);
        r.add("spring.datasource.username", SharedContainers.POSTGRES::getUsername);
        r.add("spring.datasource.password", SharedContainers.POSTGRES::getPassword);
        r.add("spring.kafka.bootstrap-servers", SharedContainers.KAFKA::getBootstrapServers);
        r.add("spring.jpa.hibernate.ddl-auto", () -> "validate");
        r.add("spring.flyway.baseline-on-migrate", () -> "true");
        r.add("spring.flyway.baseline-version", () -> "0");
        r.add("dlt.deck.ingestion.enabled", () -> "false");
    }

    @Autowired
    private WebApplicationContext wac;

    @Autowired
    private DltMessageRepository messageRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void enforcesCspNoStoreCsrfFourEyesToctouAndCryptographicAuditChain() throws Exception {
        String dltTopic = "sec-orders.dlt";
        String targetTopic = "sec-orders-" + UUID.randomUUID().toString().substring(0, 8);
        SharedContainers.createTopics(Map.of(dltTopic, 1, targetTopic, 1));
        SharedContainers.truncateDltDeckTables(jdbcTemplate);

        UUID msgId = UUID.randomUUID();
        byte[] payload = "{\"orderId\":\"ORD-SEC-1\",\"currency\":\"TRL\"}".getBytes(StandardCharsets.UTF_8);
        messageRepository.ingestBatch(List.of(new IngestedDltMessage(
                msgId, dltTopic, 0, 1L, targetTopic, 0, 1L, "cg", "k-sec".getBytes(StandardCharsets.UTF_8),
                1, "kfp-sec", payload, PayloadFormat.JSON, null, payload.length,
                "fp-sec", "ValidationException", "ValidationException", "bad currency", "stack",
                FailureCategory.VALIDATION, "patch", 0, MessageStatus.NEW, null, Map.of(), Instant.now())));

        MockMvc mvc = MockMvcBuilders.webAppContextSetup(wac).apply(springSecurity()).build();
        JsonMapper mapper = JsonMapper.builder().build();

        // 1. UI serves Strict Nonce CSP (zero 'unsafe-inline') + Cache-Control: no-store + X-Frame-Options: DENY
        MvcResult uiRes = mvc.perform(get("/dlt-deck").with(httpBasic("viewer", "pass")))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Security-Policy", Matchers.containsString("'nonce-")))
                .andExpect(header().string("Content-Security-Policy", Matchers.not(Matchers.containsString("unsafe-inline"))))
                .andExpect(header().string("Cache-Control", Matchers.containsString("no-store")))
                .andExpect(header().string("X-Frame-Options", "DENY"))
                .andReturn();
        assertThat(uiRes.getResponse().getContentAsString()).contains("spring-dlt-deck");

        String draftBody = """
                {"name":"Fix Currency","dltTopicFilter":"%s","fingerprintFilter":"fp-sec","targetTopic":"%s","orderingMode":"PRESERVE_KEY_ORDER","payloadPatchJson":"{\\"currency\\":\\"TRY\\"}","rateLimitPerSec":50,"maxRecords":10}
                """.formatted(dltTopic, targetTopic);

        // 2. Mutation WITHOUT CSRF token MUST fail with 403
        mvc.perform(post("/dlt-deck/api/plans")
                        .with(httpBasic("operator", "pass"))
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(draftBody))
                .andExpect(status().isForbidden());

        // 3. Operator drafts plan WITH CSRF token -> 201 Created (PENDING_APPROVAL + frozen planSpecHash)
        MvcResult draftRes = mvc.perform(post("/dlt-deck/api/plans")
                        .with(httpBasic("operator", "pass"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(draftBody))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("PENDING_APPROVAL"))
                .andExpect(jsonPath("$.planSpecHash").isNotEmpty())
                .andReturn();
        JsonNode planJson = mapper.readTree(draftRes.getResponse().getContentAsString());
        String planId = planJson.get("id").asString();
        String frozenHash = planJson.get("planSpecHash").asString();

        // 4. Self-approval by operator -> 403 Forbidden
        mvc.perform(post("/dlt-deck/api/plans/" + planId + "/approve")
                        .with(httpBasic("operator", "pass"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedSpecHash\":\"" + frozenHash + "\",\"executeAsync\":false}"))
                .andExpect(status().isForbidden());

        // 5. TOCTOU attack: approver submits mismatched expectedSpecHash -> 409 Conflict RFC 9457 ProblemDetail
        mvc.perform(post("/dlt-deck/api/plans/" + planId + "/approve")
                        .with(httpBasic("approver", "pass"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedSpecHash\":\"tampered_hash_0000\",\"executeAsync\":false}"))
                .andExpect(status().isConflict())
                .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
                .andExpect(jsonPath("$.errorCode").value("EXPECTED_SPEC_HASH_MISMATCH"));

        // 6. Independent approver with frozen planSpecHash -> 200 OK & COMPLETED!
        mvc.perform(post("/dlt-deck/api/plans/" + planId + "/approve")
                        .with(httpBasic("approver", "pass"))
                        .with(csrf())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"expectedSpecHash\":\"" + frozenHash + "\",\"executeAsync\":false}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.totalPublished").value(1));

        // 7. Cryptographic audit ledger & Merkle checkpoint verify cleanly
        mvc.perform(get("/dlt-deck/api/audit/verify").with(httpBasic("viewer", "pass")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.valid").value(true))
                .andExpect(jsonPath("$.totalChecked").value(Matchers.greaterThanOrEqualTo(3)));
    }
}
