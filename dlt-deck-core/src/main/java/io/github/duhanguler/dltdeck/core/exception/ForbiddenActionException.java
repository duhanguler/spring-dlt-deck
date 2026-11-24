package io.github.duhanguler.dltdeck.core.exception;

public final class ForbiddenActionException extends DltDeckException {
    public ForbiddenActionException(String errorCode, String message) {
        super(errorCode, message);
    }
}
