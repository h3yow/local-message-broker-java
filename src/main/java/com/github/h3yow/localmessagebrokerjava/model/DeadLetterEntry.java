package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 死信队列中的一条记录。
 *
 * @param topic           主题
 * @param group           消费组
 * @param offset          原始消息偏移量
 * @param payload         原始消息体
 * @param attempts        进入死信前的总投递次数
 * @param lastReason      最后一次失败原因
 * @param deadLetteredAt  进入死信的时间戳
 */
public record DeadLetterEntry(String topic, String group, long offset, String payload,
                              int attempts, String lastReason, long deadLetteredAt) {
}
