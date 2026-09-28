package com.github.h3yow.localmessagebrokerjava.model;

import java.util.List;

/**
 * 消费组状态视图（可观测/测试用）。
 *
 * @param topic             主题
 * @param group             消费组
 * @param committedOffset   已连续确认的最高位置（下一待处理偏移量）
 * @param inflightOffsets   当前在途（已投递未确认）的偏移量
 * @param deadLetterOffsets 已进入死信的偏移量
 * @param nextDeliverySeq   下一次投递序号
 */
public record GroupStatus(String topic, String group, long committedOffset, List<Long> inflightOffsets,
                          List<Long> deadLetterOffsets, long nextDeliverySeq) {
}
