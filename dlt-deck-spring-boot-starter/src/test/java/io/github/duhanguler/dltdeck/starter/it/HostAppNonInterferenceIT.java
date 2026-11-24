package io.github.duhanguler.dltdeck.starter.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.httpBasic;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.WebApplicationContext;

/**
 * Proves strict starter isolation: a host Spring Boot 4.2 application with its own Hibernate 7 {@code @Entity},
 * Flyway {@code V1__host_orders.sql}, {@code KafkaTemplate}, {@code RestController}, and {@code SecurityFilterChain}
 * runs side-by-side with {@code spring-dlt-deck} with zero bean, schema, or security collisions.
 */
@SpringBootTest(classes = HostAppNonInterferenceIT.HostTestApp.class)
class HostAppNonInterferenceIT {

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
    private HostOrderRepo hostOrderRepo;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void hostJpaFlywaySecurityAndDltDeckCoexistWithoutCollision() throws Exception {
        // 1. Host Flyway history and dlt-deck schema history are strictly isolated
        Integer flywayCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM flyway_schema_history WHERE script = 'V1__host_orders.sql'", Integer.class);
        Integer dltHistoryCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM dlt_deck_schema_history WHERE version = 'V001'", Integer.class);
        assertThat(flywayCount).isEqualTo(1);
        assertThat(dltHistoryCount).isEqualTo(1);

        // 2. Host Hibernate 7 Entity persists cleanly
        HostOrderEntity saved = hostOrderRepo.save(new HostOrderEntity("ORD-HOST-1", "CUST-9", 4500L));
        assertThat(hostOrderRepo.findById(saved.id)).isPresent();

        // 3. Host public endpoint works untouched, while /dlt-deck enforces fail-closed RBAC
        MockMvc mvc = MockMvcBuilders.webAppContextSetup(wac).apply(springSecurity()).build();
        mvc.perform(get("/api/host/ping"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.host").value("ok"));

        mvc.perform(get("/dlt-deck/api/overview"))
                .andExpect(status().isUnauthorized());

        mvc.perform(get("/dlt-deck/api/overview").with(httpBasic("viewer", "pass")))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.podId").isNotEmpty());
    }

    @Entity
    @Table(name = "host_orders")
    public static class HostOrderEntity {
        @Id
        public String id;
        public String customerId;
        public long amountCents;

        protected HostOrderEntity() {}

        public HostOrderEntity(String id, String customerId, long amountCents) {
            this.id = id;
            this.customerId = customerId;
            this.amountCents = amountCents;
        }
    }

    public interface HostOrderRepo extends JpaRepository<HostOrderEntity, String> {}

    @SpringBootApplication
    @EntityScan(basePackageClasses = HostAppNonInterferenceIT.class)
    @EnableJpaRepositories(
            basePackageClasses = HostAppNonInterferenceIT.class,
            considerNestedRepositories = true)
    public static class HostTestApp {

        @RestController
        static class HostPingController {
            @GetMapping("/api/host/ping")
            public java.util.Map<String, String> ping() {
                return java.util.Map.of("host", "ok");
            }
        }

        @Bean
        @Order(100)
        SecurityFilterChain hostSecurityChain(HttpSecurity http) throws Exception {
            return http.securityMatcher("/api/host/**")
                    .authorizeHttpRequests(a -> a.anyRequest().permitAll())
                    .csrf(c -> c.disable())
                    .build();
        }

        @Bean
        UserDetailsService users() {
            return new InMemoryUserDetailsManager(
                    User.withUsername("viewer").password("{noop}pass").roles("DLT_VIEWER").build(),
                    User.withUsername("operator").password("{noop}pass").roles("DLT_OPERATOR").build(),
                    User.withUsername("approver").password("{noop}pass").roles("DLT_APPROVER").build());
        }
    }
}
