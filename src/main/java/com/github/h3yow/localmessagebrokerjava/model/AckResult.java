package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 确认结果。
 *
 * @param offset             被确认的偏移量
 * @param duplicateAck       是否为重复/过期确认（不产生新效果）
 * @param committedPosition  确认后该消费组的已提交消费位置
 */
public record AckResult(long offset, boolean duplicateAck, long committedPosition) {
}
