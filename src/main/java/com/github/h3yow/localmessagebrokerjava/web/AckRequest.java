package com.github.h3yow.localmessagebrokerjava.web;

/**
 * 确认/否定确认请求。
 *
 * @param group   消费组（必填）
 * @param leaseId 拉取时返回的租约标识（必填）
 * @param reason  失败原因（nack 时使用）
 */
public record AckRequest(String group, Long leaseId, String reason) {
}
