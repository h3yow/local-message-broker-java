package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 投递给消费者的一条消息视图。
 *
 * @param topic      主题
 * @param group      消费组
 * @param offset     消息在主题中的偏移量
 * @param payload    消息体
 * @param attempt    当前是第几次投递（从 1 开始）
 * @param deliveryId 本次投递的唯一标识（同一 offset 重新投递会换新 id）
 */
public record DeliveryView(String topic, String group, long offset, String payload,
                           int attempt, long deliveryId) {
}
