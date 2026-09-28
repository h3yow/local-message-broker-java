package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 一次拉取投递的视图。
 *
 * @param message       被投递的消息
 * @param group         消费组
 * @param consumer      消费者标识
 * @param deliveryCount 第几次投递（首次为 1，重试递增）
 * @param leaseId       本次投递租约标识
 */
public record Delivery(Message message, String group, String consumer, int deliveryCount, long leaseId) {
}
