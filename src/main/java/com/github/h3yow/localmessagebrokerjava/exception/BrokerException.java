package com.github.h3yow.localmessagebrokerjava.exception;

/**
 * 业务异常：携带可区分的错误码与原因说明。
 */
public class BrokerException extends RuntimeException {

    private final BrokerErrorCode code;

    public BrokerException(BrokerErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public BrokerErrorCode getCode() {
        return code;
    }
}
