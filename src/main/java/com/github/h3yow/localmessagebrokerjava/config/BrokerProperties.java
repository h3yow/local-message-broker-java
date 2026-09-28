package com.github.h3yow.localmessagebrokerjava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Broker 可调参数（容量、重试、去重窗口），均可通过 application.properties 覆盖。
 */
@ConfigurationProperties(prefix = "broker")
public class BrokerProperties {

    /** 每个主题的最大消息条数（发布背压上限）。 */
    private int topicCapacity = 1000;

    /** 最大投递次数，超过后消息进入死信。 */
    private int maxDeliveryAttempts = 3;

    /** 失败重试前的固定退避毫秒数。 */
    private long retryBackoffMillis = 100L;

    /** 去重键有效窗口毫秒数。 */
    private long dedupWindowMillis = 60_000L;

    public int getTopicCapacity() {
        return topicCapacity;
    }

    public void setTopicCapacity(int topicCapacity) {
        this.topicCapacity = topicCapacity;
    }

    public int getMaxDeliveryAttempts() {
        return maxDeliveryAttempts;
    }

    public void setMaxDeliveryAttempts(int maxDeliveryAttempts) {
        this.maxDeliveryAttempts = maxDeliveryAttempts;
    }

    public long getRetryBackoffMillis() {
        return retryBackoffMillis;
    }

    public void setRetryBackoffMillis(long retryBackoffMillis) {
        this.retryBackoffMillis = retryBackoffMillis;
    }

    public long getDedupWindowMillis() {
        return dedupWindowMillis;
    }

    public void setDedupWindowMillis(long dedupWindowMillis) {
        this.dedupWindowMillis = dedupWindowMillis;
    }
}
