package io.github.duhanguler.dltdeck.demo;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.web.servlet.config.annotation.ApiVersionConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class DltDeckDemoApplication implements WebMvcConfigurer {

    public static void main(String[] args) {
        SpringApplication.run(DltDeckDemoApplication.class, args);
    }

    /**
     * Spring Framework 7 Native API Versioning configured strictly in the host application layer
     * (never inside {@code dlt-deck-spring-boot-starter}).
     */
    @Override
    public void configureApiVersioning(ApiVersionConfigurer configurer) {
        configurer.useRequestHeader("X-API-Version").setDefaultVersion("1.0");
    }

    @Bean
    NewTopic paymentsEventsTopic() {
        return TopicBuilder.name("payments.events").partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic paymentsDltTopic() {
        return TopicBuilder.name("payments.events.dlt").partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic paymentsCanaryTopic() {
        return TopicBuilder.name("payments.events.canary").partitions(1).replicas(1).build();
    }

    @Bean
    @Order(100)
    SecurityFilterChain demoHostSecurityFilterChain(HttpSecurity http) throws Exception {
        return http.securityMatcher("/api/**", "/actuator/**")
                .authorizeHttpRequests(a -> a.anyRequest().permitAll())
                .csrf(c -> c.disable())
                .build();
    }

    @Bean
    UserDetailsService demoUsers() {
        return new InMemoryUserDetailsManager(
                User.withUsername("viewer").password("{noop}viewer123").roles("DLT_VIEWER").build(),
                User.withUsername("operator").password("{noop}operator123").roles("DLT_OPERATOR").build(),
                User.withUsername("approver").password("{noop}approver123").roles("DLT_APPROVER").build());
    }
}
