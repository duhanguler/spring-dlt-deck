package io.github.duhanguler.dltdeck.starter.autoconfigure;

import io.github.duhanguler.dltdeck.starter.security.DltDeckSecurityConfiguration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Import;
import org.springframework.security.web.SecurityFilterChain;

@AutoConfiguration(
        before = DltDeckWebAutoConfiguration.class,
        afterName = {
            "org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration",
            "org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration"
        })
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@ConditionalOnProperty(prefix = "dlt.deck", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass(SecurityFilterChain.class)
@Import(DltDeckSecurityConfiguration.class)
public class DltDeckSecurityAutoConfiguration {}
