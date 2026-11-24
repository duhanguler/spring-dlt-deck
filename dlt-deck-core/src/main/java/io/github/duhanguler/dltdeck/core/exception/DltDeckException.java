package io.github.duhanguler.dltdeck.core.exception;

import org.jspecify.annotations.Nullable;

/**
 * Sealed root exception hierarchy mapped cleanly to RFC 9457 ProblemDetail in the web adapter.
 */
public abstract sealed class DltDeckException extends RuntimeException
        permits NotFoundException,
                ForbiddenActionException,
                PlanConflictException,
                ValidationDomainException,
                RateLimitExceededException {

    private final String errorCode;

    protected DltDeckException(String errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    protected DltDeckException(String errorCode, String message, @Nullable Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public String errorCode() {
        return errorCode;
    }
}
