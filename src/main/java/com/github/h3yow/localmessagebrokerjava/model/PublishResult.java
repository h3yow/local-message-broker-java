package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 发布结果。
 *
 * @param offset    消息在主题中的偏移量（去重命中时为首次发布的偏移量）
 * @param duplicate 是否命中去重窗口（true 表示本次发布未产生新消息）
 */
public record PublishResult(long offset, boolean duplicate) {
}
