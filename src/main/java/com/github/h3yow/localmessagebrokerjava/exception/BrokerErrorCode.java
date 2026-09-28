package com.github.h3yow.localmessagebrokerjava.exception;

/**
 * Broker 错误码：调用方需要能区分不同的拒绝/失败原因。
 */
public enum BrokerErrorCode {
    /** 主题不存在 */
    TOPIC_NOT_FOUND,
    /** 消费组不存在 */
    GROUP_NOT_FOUND,
    /** 队列容量已满（瞬时背压，重试可能成功） */
    CAPACITY_FULL,
    /** 消息本身非法（空内容、超长等） */
    INVALID_MESSAGE,
    /** 请求参数非法（空主题/空消费组等） */
    INVALID_REQUEST,
    /** 偏移量不合法（超范围） */
    INVALID_OFFSET,
    /** 试图把消费位置回退，明确拒绝 */
    SEEK_BACKWARD_REJECTED,
    /** 在途租约不存在或已过期（消息未投递给该消费者，或租约已被回收） */
    LEASE_NOT_FOUND
}
