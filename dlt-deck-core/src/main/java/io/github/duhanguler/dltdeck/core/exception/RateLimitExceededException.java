package io.github.duhanguler.dltdeck.core.exception;

public final class RateLimitExceededException extends DltDeckException {
    public RateLimitExceededException(String errorCode, String message) {
        super(errorCode, message);
    }
}
