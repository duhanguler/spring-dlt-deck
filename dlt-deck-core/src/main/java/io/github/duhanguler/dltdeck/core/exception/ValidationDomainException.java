package io.github.duhanguler.dltdeck.core.exception;

import org.jspecify.annotations.Nullable;

public final class ValidationDomainException extends DltDeckException {
    public ValidationDomainException(String errorCode, String message) {
        super(errorCode, message);
    }

    public ValidationDomainException(String errorCode, String message, @Nullable Throwable cause) {
        super(errorCode, message, cause);
    }
}
