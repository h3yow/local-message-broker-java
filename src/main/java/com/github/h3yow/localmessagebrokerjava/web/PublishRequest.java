package com.github.h3yow.localmessagebrokerjava.web;

/**
 * 发布请求。
 *
 * @param payload  消息内容（必填）
 * @param dedupKey 去重键（可空）
 */
public record PublishRequest(String payload, String dedupKey) {
}
