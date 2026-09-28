package com.github.h3yow.localmessagebrokerjava.web;

/**
 * 统一错误响应。
 *
 * @param code    机器可区分的错误码
 * @param reason  人类可读的拒绝/失败原因
 */
public record ErrorResponse(String code, String reason) {
}
