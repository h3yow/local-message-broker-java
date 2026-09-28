package com.github.h3yow.localmessagebrokerjava.model;

/**
 * 消费组状态视图，用于日志与可观测性。
 *
 * @param committedPosition 已连续确认到的位置（下一个待提交偏移量）
 * @param inFlightCount     当前已投递未确认的消息数
 * @param deadLetterCount   该组死信条数
 */
public record GroupStatus(long committedPosition, int inFlightCount, int deadLetterCount) {
}
