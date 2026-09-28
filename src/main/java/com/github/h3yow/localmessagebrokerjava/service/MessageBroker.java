package com.github.h3yow.localmessagebrokerjava.service;

import com.github.h3yow.localmessagebrokerjava.model.AckResult;
import com.github.h3yow.localmessagebrokerjava.model.DeadLetterEntry;
import com.github.h3yow.localmessagebrokerjava.model.DeliveryView;
import com.github.h3yow.localmessagebrokerjava.model.GroupStatus;
import com.github.h3yow.localmessagebrokerjava.model.PublishResult;

import java.util.List;

/**
 * 本地进程内消息队列对外提供的核心能力。
 */
public interface MessageBroker {

    /** 创建主题；已存在则拒绝。 */
    void createTopic(String topic, int capacity);

    /** 发布消息；主题满 / 主题不存在 / 消息非法 / 去重命中均通过结果或异常区分。 */
    PublishResult publish(String topic, String payload, String dedupKey);

    /** 消费组订阅主题（从最新位置之后开始）。重复订阅需被拒绝。 */
    void subscribe(String topic, String group);

    /**
     * 拉取一条消息。未确认的消息不会被重复分给同组其它消费者；没有可投递消息时返回 null。
     */
    DeliveryView pull(String topic, String group);

    /** 确认一条消息。重复/乱序确认必须幂等且只能让消费位置前进。 */
    AckResult acknowledge(String topic, String group, long offset);

    /**
     * 否定确认：按原因进入重试；达到投递上限后进入死信。
     */
    void negativeAcknowledge(String topic, String group, long offset, String reason);

    /**
     * 消费组组件重新初始化：把所有已投递未确认的消息退回可重新投递状态，保证不丢消息。
     */
    int reinitialize(String topic, String group);

    /**
     * 调整消费位置。任何会导致位置回退或跳过未确认消息的请求都必须被明确拒绝。
     */
    long seek(String topic, String group, long targetOffset);

    /** 查看消费组状态（消费位置 / 在途数 / 死信数）。 */
    GroupStatus status(String topic, String group);

    /** 读取某主题某组的死信列表（只读视图）。 */
    List<DeadLetterEntry> deadLetters(String topic, String group);
}
