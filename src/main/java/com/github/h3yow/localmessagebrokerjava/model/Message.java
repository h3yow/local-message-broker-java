package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 主题内的一条不可变消息。
 *
 * @param offset   消息在主题内的偏移量（单调递增，从 0 开始）
 * @param payload  消息内容
 * @param dedupKey 去重键（可空，空表示不参与去重）
 */
public record Message(long offset, String payload, String dedupKey) {
}
