package com.github.h3yow.localmessagebrokerjava.exception;

/**
 * 业务错误码，每一类失败原因都可被调用方明确区分。
 */
public enum ErrorCode {
    TOPIC_NOT_FOUND,
    TOPIC_ALREADY_EXISTS,
    QUEUE_FULL,
    INVALID_MESSAGE,
    INVALID_DELIVERY,
    DUPLICATE_KEY,
    GROUP_NOT_FOUND,
    GROUP_ALREADY_SUBSCRIBED,
    SEEK_REJECTED,
    MESSAGE_DEAD_LETTERED,
    RETRY_LIMIT_REACHED,
    INTERNAL_ERROR
}
