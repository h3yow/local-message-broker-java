package com.github.h3yow.localmessagebrokerjava.web;

/**
 * 移动消费位置请求。
 *
 * @param group  消费组（必填）
 * @param offset 目标偏移量（必填，只允许不小于当前已确认位置）
 */
public record SeekRequest(String group, Long offset) {
}
