package io.github.duhanguler.dltdeck.starter.autoconfigure;

import io.github.duhanguler.dltdeck.core.spi.ReplayRateLimitCoordinator;
import io.github.duhanguler.dltdeck.storage.redis.RedisLuaRateLimitCoordinator;
import java.time.Clock;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;

@AutoConfiguration(
        before = DltDeckCoreAutoConfiguration.class,
        afterName = {
            "org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration",
            "org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration"
        })
@ConditionalOnProperty(prefix = "dlt.deck.redis", name = "enabled", havingValue = "true")
@ConditionalOnClass({StringRedisTemplate.class, RedisLuaRateLimitCoordinator.class})
@ConditionalOnBean(StringRedisTemplate.class)
public class DltDeckRedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ReplayRateLimitCoordinator.class)
    public ReplayRateLimitCoordinator dltDeckRedisRateLimitCoordinator(
            StringRedisTemplate redisTemplate,
            @Qualifier("dltDeckInternal") org.springframework.beans.factory.ObjectProvider<Clock> clockProvider) {
        Clock clock = clockProvider.getIfAvailable(Clock::systemUTC);
        return new RedisLuaRateLimitCoordinator(redisTemplate, clock);
    }
}
