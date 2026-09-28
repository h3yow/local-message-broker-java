package com.github.h3yow.localmessagebrokerjava.exception;

/**
 * Broker 统一业务异常，携带可区分的错误码。
 */
public class BrokerException extends RuntimeException {

    private final ErrorCode errorCode;

    public BrokerException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }
}
