package com.github.h3yow.localmessagebrokerjava.web;

/**
 * 消费组注册请求。
 *
 * @param group 消费组名（必填）
 */
public record GroupRequest(String group) {
}
