package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 主题日志中的一条不可变消息。
 *
 * @param offset      主题内单调递增的偏移量
 * @param payload     消息体
 * @param dedupKey    去重键，可为空
 * @param publishedAt 发布时间戳（毫秒）
 */
public record StoredMessage(long offset, String payload, String dedupKey, long publishedAt) {
}
