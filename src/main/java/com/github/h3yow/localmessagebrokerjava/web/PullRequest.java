package com.github.h3yow.localmessagebrokerjava.web;

/**
 * 拉取请求。
 *
 * @param group    消费组（必填）
 * @param consumer 消费者标识（必填）
 */
public record PullRequest(String group, String consumer) {
}
