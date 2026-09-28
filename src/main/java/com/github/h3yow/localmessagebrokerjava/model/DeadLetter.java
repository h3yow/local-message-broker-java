package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 死信记录。
 *
 * @param topic          主题
 * @param group          消费组
 * @param offset         消息偏移量
 * @param payload        消息内容
 * @param attempts       累计投递次数
 * @param lastReason     最后一次失败原因
 */
public record DeadLetter(String topic, String group, long offset, String payload, int attempts,
                         String lastReason) {
}
