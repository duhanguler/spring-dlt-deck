package io.github.duhanguler.dltdeck.core.exception;

public final class NotFoundException extends DltDeckException {
    public NotFoundException(String errorCode, String message) {
        super(errorCode, message);
    }
}
