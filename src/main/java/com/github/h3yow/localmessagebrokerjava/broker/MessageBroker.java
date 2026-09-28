package com.github.h3yow.localmessagebrokerjava.broker;

import com.github.h3yow.localmessagebrokerjava.model.DeadLetter;
import com.github.h3yow.localmessagebrokerjava.model.Delivery;
import com.github.h3yow.localmessagebrokerjava.model.GroupStatus;
import com.github.h3yow.localmessagebrokerjava.model.PublishResult;

import java.util.List;
import java.util.Optional;

/**
 * 本地进程内消息队列核心接口。
 *
 * <p>投递语义：至少一次（at-least-once）。
 * 同组内一条消息同一时刻只会被一个消费者拿到；不同消费组各自独立消费完整消息。
 */
public interface MessageBroker {

    /** 创建主题（幂等：已存在则无效果）。 */
    void createTopic(String topic);

    /**
     * 注册消费组（幂等：已存在则无效果）。
     *
     * <p>组应在消息发布前/消费前注册：消息只有被所有已注册组连续越过后才允许物理回收，
     * 因此先注册的组保证能独立消费完整消息；首次 pull 时也会惰性自动建组，
     * 但自动建组只能看到当时仍保留在内存中的消息。
     */
    void createGroup(String topic, String group);

    /**
     * 发布消息。
     *
     * @param topic    主题
     * @param payload  消息内容
     * @param dedupKey 去重键，可空
     * @return 发布结果（新消息偏移量，或去重命中时的已有偏移量）
     */
    PublishResult publish(String topic, String payload, String dedupKey);

    /**
     * 拉取消息：同组内一条消息只会被一个消费者拿到（在途期间其他人拉不到）。
     *
     * @param topic    主题
     * @param group    消费组（不存在则自动创建）
     * @param consumer 消费者标识
     * @return 一条可消费消息；当前没有可消费消息时为空
     */
    Optional<Delivery> pull(String topic, String group, String consumer);

    /**
     * 确认消息。幂等：重复确认不会产生副作用。
     *
     * @param topic   主题
     * @param group   消费组
     * @param offset  消息偏移量
     * @param leaseId 拉取时拿到的租约标识
     */
    void ack(String topic, String group, long offset, long leaseId);

    /**
     * 否定确认：消费失败。未达重试上限则消息重新可投递（投递次数 +1）；
     * 达到上限则进入死信。
     *
     * @param topic   主题
     * @param group   消费组
     * @param offset  消息偏移量
     * @param leaseId 拉取时拿到的租约标识
     * @param reason  可区分的失败原因
     */
    void nack(String topic, String group, long offset, long leaseId, String reason);

    /**
     * 移动消费位置。只允许向前（targetOffset 必须不小于当前已确认位置）；
     * 回退请求必须被明确拒绝。
     */
    void seek(String topic, String group, long targetOffset);

    /**
     * 组件重新初始化：把所有消费组当前在途（未确认）的消息全部退回，
     * 使其可以被重新投递。已确认位置与死信保持不变。
     *
     * @return 被退回重新投递的消息总数
     */
    int reinitializeInflight();

    /** 查询某组的死信。 */
    List<DeadLetter> deadLetters(String topic, String group);

    /** 查询某组状态（可观测/断言用）。 */
    GroupStatus groupStatus(String topic, String group);
}
