package io.github.duhanguler.dltdeck.core.context;

import java.util.Objects;
import java.util.concurrent.Callable;
import org.jspecify.annotations.Nullable;

/**
 * Immutable security & audit actor propagated via Java 25 final {@link ScopedValue} (JEP 506).
 * Uses Java 25 Flexible Constructor Bodies (JEP 513) for eager validation prior to canonical assignment.
 */
public record ActorContext(String actorId, ActorRole role, @Nullable String correlationId) {

    public static final ScopedValue<ActorContext> CURRENT = ScopedValue.newInstance();

    public static final ActorContext SYSTEM_ACTOR = new ActorContext("system", ActorRole.SYSTEM, null);

    public ActorContext(String actorId, ActorRole role, @Nullable String correlationId) {
        Objects.requireNonNull(actorId, "actorId must not be null");
        String trimmedActor = actorId.trim();
        if (trimmedActor.isEmpty() || trimmedActor.length() > 128) {
            throw new IllegalArgumentException("actorId must be 1..128 non-blank characters");
        }
        Objects.requireNonNull(role, "role must not be null");
        String normalizedCorrelation = correlationId == null || correlationId.isBlank()
                ? null
                : correlationId.trim();
        this.actorId = trimmedActor;
        this.role = role;
        this.correlationId = normalizedCorrelation;
    }

    public static ActorContext current() {
        if (!CURRENT.isBound()) {
            throw new IllegalStateException("No ActorContext is bound in current ScopedValue scope");
        }
        return CURRENT.get();
    }

    public static ActorContext currentOrSystem() {
        return CURRENT.isBound() ? CURRENT.get() : SYSTEM_ACTOR;
    }

    public static void runAs(ActorContext actor, Runnable action) {
        Objects.requireNonNull(actor, "actor must not be null");
        Objects.requireNonNull(action, "action must not be null");
        ScopedValue.where(CURRENT, actor).run(action);
    }

    public static <T> T callAs(ActorContext actor, Callable<T> action) throws Exception {
        Objects.requireNonNull(actor, "actor must not be null");
        Objects.requireNonNull(action, "action must not be null");
        return ScopedValue.where(CURRENT, actor).call(action::call);
    }
}
