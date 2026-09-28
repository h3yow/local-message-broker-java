package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 发布结果。
 *
 * @param offset  消息在主题内的偏移量；命中去重时返回已存在消息的偏移量
 * @param deduped 是否因去重而被折叠（true 表示没有产生新消息）
 */
public record PublishResult(long offset, boolean deduped) {
}
