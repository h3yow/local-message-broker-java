package com.github.h3yow.localmessagebrokerjava.broker;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Broker 可调参数（application.properties 中以 broker.* 覆盖）。
 */
@ConfigurationProperties(prefix = "broker")
public class BrokerProperties {

    /** 每个主题的容量上限（未被所有消费组确认前可保留的最大消息条数）。 */
    private int topicCapacity = 1000;

    /** 单条消息内容最大字节数（按 UTF-8 计）。 */
    private int maxPayloadBytes = 65536;

    /** 进入死信前允许的最大投递次数（达到此次数后再 nack 即进死信）。 */
    private int maxDeliveryAttempts = 3;

    /** 去重键有效窗口（毫秒）；窗口内相同去重键的发布会被折叠。 */
    private long dedupWindowMillis = 5 * 60 * 1000L;

    public int getTopicCapacity() {
        return topicCapacity;
    }

    public void setTopicCapacity(int topicCapacity) {
        this.topicCapacity = topicCapacity;
    }

    public int getMaxPayloadBytes() {
        return maxPayloadBytes;
    }

    public void setMaxPayloadBytes(int maxPayloadBytes) {
        this.maxPayloadBytes = maxPayloadBytes;
    }

    public int getMaxDeliveryAttempts() {
        return maxDeliveryAttempts;
    }

    public void setMaxDeliveryAttempts(int maxDeliveryAttempts) {
        this.maxDeliveryAttempts = maxDeliveryAttempts;
    }

    public long getDedupWindowMillis() {
        return dedupWindowMillis;
    }

    public void setDedupWindowMillis(long dedupWindowMillis) {
        this.dedupWindowMillis = dedupWindowMillis;
    }
}
