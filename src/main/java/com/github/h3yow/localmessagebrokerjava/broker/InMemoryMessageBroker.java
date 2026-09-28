package com.github.h3yow.localmessagebrokerjava.broker;

import com.github.h3yow.localmessagebrokerjava.exception.BrokerErrorCode;
import com.github.h3yow.localmessagebrokerjava.exception.BrokerException;
import com.github.h3yow.localmessagebrokerjava.model.DeadLetter;
import com.github.h3yow.localmessagebrokerjava.model.Delivery;
import com.github.h3yow.localmessagebrokerjava.model.GroupStatus;
import com.github.h3yow.localmessagebrokerjava.model.Message;
import com.github.h3yow.localmessagebrokerjava.model.PublishResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 纯内存的本地消息队列实现。
 *
 * <h3>并发模型</h3>
 * 每个主题一把锁（{@link TopicState#lock}），主题内的消息存储、消费组状态、去重表
 * 全部在该锁保护下访问；不同主题之间完全并行。组内“已确认位置”用一个连续位点
 * {@code committed}（语义为“下一个待处理偏移量”）加一个乱序完成集合推进：
 * 只有连续前缀才能折叠进 committed，因此并发确认、乱序确认、重复确认都只能让位置
 * 往前，不会回退、不会重复生效。
 *
 * <h3>投递语义</h3>
 * 至少一次：消息被拉取后进入在途（inflight）表，其他同组消费者拉不到；nack 或
 * 组件重新初始化会让它重新可投，投递次数单调递增；达到上限的 nack 转入死信。
 *
 * <h3>容量与去重</h3>
 * 每个主题有消息条数上限；只有“所有消费组都已连续越过”的前缀才允许被物理删除
 * 以释放容量，删除后仍满则以 CAPACITY_FULL 拒绝发布。去重键在时间窗口内命中时
 * 折叠为已有偏移量，不产生新消息。
 */
@Component
public class InMemoryMessageBroker implements MessageBroker {

    private static final Logger log = LoggerFactory.getLogger(InMemoryMessageBroker.class);

    private final BrokerProperties properties;
    private final Map<String, TopicState> topics = new ConcurrentHashMap<>();

    public InMemoryMessageBroker(BrokerProperties properties) {
        this.properties = properties;
    }

    /** 单条在途租约。 */
    private static final class Inflight {
        final String consumer;
        final long leaseId;

        Inflight(String consumer, long leaseId) {
            this.consumer = consumer;
            this.leaseId = leaseId;
        }
    }

    /** 去重窗口内的一条记录。 */
    private static final class DedupEntry {
        final long offset;
        final long createdAt;

        DedupEntry(long offset, long createdAt) {
            this.offset = offset;
            this.createdAt = createdAt;
        }
    }

    /** 单个消费组在某主题上的消费状态。 */
    private static final class GroupState {
        /** 连续已确认/已死信的最高位置 + 1，即下一个待处理偏移量；只进不退。 */
        long committed;
        /** 已完成但尚未折叠成连续前缀的偏移量（乱序确认产生的空洞之后的部分）。 */
        final TreeSet<Long> completed = new TreeSet<>();
        /** 当前在途：offset -> 租约。同组同一时刻一条消息只会在一个租约里。 */
        final Map<Long, Inflight> inflight = new TreeMap<>();
        /** 每个偏移量已经发生过的投递次数（重新投递时 +1）。 */
        final Map<Long, Integer> deliveryCounts = new TreeMap<>();
        /** 死信偏移量集合。 */
        final TreeSet<Long> deadOffsets = new TreeSet<>();
        /** 死信明细（按进入顺序）。 */
        final List<DeadLetter> deadLetters = new ArrayList<>();
        /** 单调递增的租约号生成器，也作为状态观测中的投递序号。 */
        long deliverySeq;
    }

    /** 单个主题的全部状态，由 {@link #lock} 串行保护。 */
    private static final class TopicState {
        final ReentrantLock lock = new ReentrantLock();
        final String name;
        /** 保留中的消息，baseOffset 为队首消息的偏移量。 */
        final ArrayDeque<Message> messages = new ArrayDeque<>();
        long baseOffset;
        long nextOffset;
        final Map<String, GroupState> groups = new TreeMap<>();
        final Map<String, DedupEntry> dedup = new HashMap<>();

        TopicState(String name) {
            this.name = name;
        }
    }

    @Override
    public void createTopic(String topic) {
        if (isBlank(topic)) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "topic 不能为空");
        }
        topics.computeIfAbsent(topic, TopicState::new);
        log.info("topic 已创建（或已存在）: topic={}", topic);
    }

    @Override
    public void createGroup(String topic, String group) {
        TopicState ts = requireTopic(topic);
        if (isBlank(group)) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "group 不能为空");
        }
        ts.lock.lock();
        try {
            boolean created = ts.groups.putIfAbsent(group, new GroupState()) == null;
            log.info("消费组{}: topic={}, group={}", created ? "已注册" : "已存在（幂等）", topic, group);
        } finally {
            ts.lock.unlock();
        }
    }

    @Override
    public PublishResult publish(String topic, String payload, String dedupKey) {
        if (isBlank(topic)) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "topic 不能为空");
        }
        TopicState ts = topics.get(topic);
        if (ts == null) {
            throw new BrokerException(BrokerErrorCode.TOPIC_NOT_FOUND, "主题不存在: " + topic);
        }
        validatePayload(payload);

        long now = System.currentTimeMillis();
        ts.lock.lock();
        try {
            // 1) 去重：先惰性清理过期键，再查窗口内命中。
            if (dedupKey != null) {
                evictExpiredDedup(ts, now);
                DedupEntry hit = ts.dedup.get(dedupKey);
                if (hit != null) {
                    log.info("发布被去重折叠: topic={}, dedupKey={}, 复用offset={}", topic, dedupKey, hit.offset);
                    return new PublishResult(hit.offset, true);
                }
            }
            // 2) 先物理删除“所有组都已越过”的前缀以释放容量，再判满。
            evictConsumedPrefix(ts);
            if (ts.messages.size() >= properties.getTopicCapacity()) {
                log.warn("发布被拒绝: topic={}, reason=CAPACITY_FULL, 当前保留={}, 容量上限={}, 去重键={}",
                        topic, ts.messages.size(), properties.getTopicCapacity(), dedupKey);
                throw new BrokerException(BrokerErrorCode.CAPACITY_FULL,
                        "主题队列瞬时写满，请稍后重试: topic=" + topic
                                + ", retained=" + ts.messages.size()
                                + ", capacity=" + properties.getTopicCapacity());
            }
            // 3) 追加消息。
            long offset = ts.nextOffset++;
            Message msg = new Message(offset, payload, dedupKey);
            ts.messages.addLast(msg);
            if (dedupKey != null) {
                ts.dedup.put(dedupKey, new DedupEntry(offset, now));
            }
            log.info("消息已发布: topic={}, offset={}, dedupKey={}", topic, offset, dedupKey);
            return new PublishResult(offset, false);
        } finally {
            ts.lock.unlock();
        }
    }

    @Override
    public Optional<Delivery> pull(String topic, String group, String consumer) {
        if (isBlank(topic)) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "topic 不能为空");
        }
        if (isBlank(group)) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "group 不能为空");
        }
        if (isBlank(consumer)) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "consumer 不能为空");
        }
        TopicState ts = requireTopic(topic);

        ts.lock.lock();
        try {
            GroupState gs = ts.groups.computeIfAbsent(group, g -> new GroupState());
            // 从连续位点开始，跳过已完成空洞与在途消息，找第一条可投递的。
            long candidate = nextDeliverableOffset(ts, gs);
            if (candidate < 0) {
                return Optional.empty();
            }
            Message msg = findMessage(ts, candidate);
            long leaseId = ++gs.deliverySeq;
            int attempts = gs.deliveryCounts.merge(candidate, 1, Integer::sum);
            gs.inflight.put(candidate, new Inflight(consumer, leaseId));
            log.info("消息投递: topic={}, group={}, consumer={}, offset={}, 第{}次投递, leaseId={}, 组消费位置committed={}",
                    topic, group, consumer, candidate, attempts, leaseId, gs.committed);
            return Optional.of(new Delivery(msg, group, consumer, attempts, leaseId));
        } finally {
            ts.lock.unlock();
        }
    }

    @Override
    public void ack(String topic, String group, long offset, long leaseId) {
        if (offset < 0) {
            throw new BrokerException(BrokerErrorCode.INVALID_OFFSET, "offset 不能为负: " + offset);
        }
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);

        ts.lock.lock();
        try {
            // 已完成（含重复确认、乱序确认后已折叠）：幂等成功，不重复生效。
            if (isCompleted(gs, offset)) {
                log.info("重复确认，忽略（幂等）: topic={}, group={}, offset={}, 当前committed={}",
                        topic, group, offset, gs.committed);
                return;
            }
            Inflight lease = gs.inflight.get(offset);
            if (lease == null || lease.leaseId != leaseId) {
                log.warn("确认被拒绝: topic={}, group={}, offset={}, 传入leaseId={}, 原因=LEASE_NOT_FOUND（租约不存在或已失效，可能已被nack/重新初始化回收）",
                        topic, group, offset, leaseId);
                throw new BrokerException(BrokerErrorCode.LEASE_NOT_FOUND,
                        "在途租约不存在或已失效: topic=" + topic + ", group=" + group
                                + ", offset=" + offset + ", leaseId=" + leaseId
                                + "（消息可能已被否定确认或被重新初始化回收，不能用旧租约确认）");
            }
            gs.inflight.remove(offset);
            complete(ts, gs, offset);
            log.info("消息已确认: topic={}, group={}, offset={}, 组消费位置推进到committed={}",
                    topic, group, offset, gs.committed);
        } finally {
            ts.lock.unlock();
        }
    }

    @Override
    public void nack(String topic, String group, long offset, long leaseId, String reason) {
        if (offset < 0) {
            throw new BrokerException(BrokerErrorCode.INVALID_OFFSET, "offset 不能为负: " + offset);
        }
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);

        ts.lock.lock();
        try {
            if (isCompleted(gs, offset)) {
                // 已终态（已确认/已死信）后到达的失败通知不能让它复活，仅记录。
                log.warn("否定确认被忽略：消息已处于终态: topic={}, group={}, offset={}, reason={}, committed={}",
                        topic, group, offset, reason, gs.committed);
                return;
            }
            Inflight lease = gs.inflight.get(offset);
            if (lease == null || lease.leaseId != leaseId) {
                log.warn("否定确认被拒绝: topic={}, group={}, offset={}, 传入leaseId={}, 失败原因={}, 拒绝原因=LEASE_NOT_FOUND",
                        topic, group, offset, leaseId, reason);
                throw new BrokerException(BrokerErrorCode.LEASE_NOT_FOUND,
                        "在途租约不存在或已失效: topic=" + topic + ", group=" + group
                                + ", offset=" + offset + ", leaseId=" + leaseId);
            }
            int attempts = gs.deliveryCounts.getOrDefault(offset, 1);
            gs.inflight.remove(offset);
            if (attempts >= properties.getMaxDeliveryAttempts()) {
                // 达到投递上限：进入死信，组消费位置可继续向前。
                Message msg = findMessage(ts, offset);
                DeadLetter dl = new DeadLetter(topic, group, offset,
                        msg == null ? null : msg.payload(), attempts, reason);
                gs.deadLetters.add(dl);
                gs.deadOffsets.add(offset);
                complete(ts, gs, offset);
                log.error("消息进入死信: topic={}, group={}, offset={}, 累计投递={}次, 最后失败原因={}, committed推进到={}",
                        topic, group, offset, attempts, reason, gs.committed);
            } else {
                // 未达上限：保持可投递，下一次 pull 会以更大的投递次数重新投出。
                log.warn("消息消费失败，将重试: topic={}, group={}, offset={}, 已投递={}次, 上限={}, 失败原因={}",
                        topic, group, offset, attempts, properties.getMaxDeliveryAttempts(), reason);
            }
        } finally {
            ts.lock.unlock();
        }
    }

    @Override
    public void seek(String topic, String group, long targetOffset) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);

        ts.lock.lock();
        try {
            if (targetOffset < 0 || targetOffset > ts.nextOffset) {
                throw new BrokerException(BrokerErrorCode.INVALID_OFFSET,
                        "目标偏移量越界: target=" + targetOffset + ", 主题下一条写入偏移量=" + ts.nextOffset);
            }
            long before = gs.committed;
            if (targetOffset < before) {
                log.warn("回退消费位置被明确拒绝: topic={}, group={}, 当前committed={}, 请求回退到={}, 拒绝原因=SEEK_BACKWARD_REJECTED",
                        topic, group, before, targetOffset);
                throw new BrokerException(BrokerErrorCode.SEEK_BACKWARD_REJECTED,
                        "消费位置只能向前推进，拒绝回退: topic=" + topic + ", group=" + group
                                + ", 当前committed=" + before + ", 请求目标=" + targetOffset
                                + "（至少一次语义下未确认消息可通过 reinitialize 重新投递，但不允许回退位点）");
            }
            if (targetOffset == before) {
                log.info("seek 无变化: topic={}, group={}, committed={}", topic, group, before);
                return;
            }
            // 向前跳过：作废旧租约，把区间内偏移量标记为完成以折叠连续位点。
            for (long off = before; off < targetOffset; off++) {
                gs.inflight.remove(off);
                gs.completed.add(off);
            }
            foldCommitted(gs);
            evictConsumedPrefix(ts);
            log.info("消费位置向前移动: topic={}, group={}, {} -> {}", topic, group, before, gs.committed);
        } finally {
            ts.lock.unlock();
        }
    }

    @Override
    public int reinitializeInflight() {
        int returned = 0;
        for (TopicState ts : topics.values()) {
            ts.lock.lock();
            try {
                for (Map.Entry<String, GroupState> e : ts.groups.entrySet()) {
                    GroupState gs = e.getValue();
                    // 快照在途偏移后逐条解除租约；投递次数保留，下次 pull 时 +1，
                    // 因此这些未确认消息会被重新投递而不是静默丢掉。
                    for (Long offset : new ArrayList<>(gs.inflight.keySet())) {
                        Inflight lease = gs.inflight.remove(offset);
                        int attempts = gs.deliveryCounts.getOrDefault(offset, 1);
                        returned++;
                        log.info("组件重新初始化：在途消息退回重投: topic={}, group={}, consumer={}, offset={}, 已投递={}次（下次为第{}次）, 旧leaseId={}",
                                ts.name, e.getKey(), lease.consumer, offset, attempts, attempts + 1, lease.leaseId);
                    }
                }
            } finally {
                ts.lock.unlock();
            }
        }
        log.info("组件重新初始化完成，共退回未确认消息 {} 条", returned);
        return returned;
    }

    @Override
    public List<DeadLetter> deadLetters(String topic, String group) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        ts.lock.lock();
        try {
            return List.copyOf(gs.deadLetters);
        } finally {
            ts.lock.unlock();
        }
    }

    @Override
    public GroupStatus groupStatus(String topic, String group) {
        TopicState ts = requireTopic(topic);
        GroupState gs = requireGroup(ts, group);
        ts.lock.lock();
        try {
            return new GroupStatus(topic, group, gs.committed,
                    List.copyOf(gs.inflight.keySet()),
                    List.copyOf(gs.deadOffsets),
                    gs.deliverySeq + 1);
        } finally {
            ts.lock.unlock();
        }
    }

    // ------------------------------------------------------------------
    // 内部辅助方法（调用方均已持有主题锁，除纯函数外）
    // ------------------------------------------------------------------

    private TopicState requireTopic(String topic) {
        if (isBlank(topic)) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "topic 不能为空");
        }
        TopicState ts = topics.get(topic);
        if (ts == null) {
            throw new BrokerException(BrokerErrorCode.TOPIC_NOT_FOUND, "主题不存在: " + topic);
        }
        return ts;
    }

    private GroupState requireGroup(TopicState ts, String group) {
        if (isBlank(group)) {
            throw new BrokerException(BrokerErrorCode.INVALID_REQUEST, "group 不能为空");
        }
        GroupState gs = ts.groups.get(group);
        if (gs == null) {
            throw new BrokerException(BrokerErrorCode.GROUP_NOT_FOUND,
                    "消费组不存在: topic=" + ts.name + ", group=" + group);
        }
        return gs;
    }

    private void validatePayload(String payload) {
        if (payload == null || payload.isEmpty()) {
            log.warn("发布被拒绝: reason=INVALID_MESSAGE, 原因=payload为空");
            throw new BrokerException(BrokerErrorCode.INVALID_MESSAGE, "消息内容不能为空");
        }
        int bytes = payload.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > properties.getMaxPayloadBytes()) {
            log.warn("发布被拒绝: reason=INVALID_MESSAGE, payload字节数={}, 上限={}",
                    bytes, properties.getMaxPayloadBytes());
            throw new BrokerException(BrokerErrorCode.INVALID_MESSAGE,
                    "消息内容超长: bytes=" + bytes + ", 上限=" + properties.getMaxPayloadBytes());
        }
    }

    /** 惰性删除过期去重键。 */
    private void evictExpiredDedup(TopicState ts, long now) {
        long window = properties.getDedupWindowMillis();
        ts.dedup.entrySet().removeIf(en -> now - en.getValue().createdAt > window);
    }

    /**
     * 物理删除所有消费组都已连续越过的消息前缀。
     * 没有任何消费组时不删除（容量背压将作用于发布端）。
     */
    private void evictConsumedPrefix(TopicState ts) {
        if (ts.groups.isEmpty() || ts.messages.isEmpty()) {
            return;
        }
        long hwm = Long.MAX_VALUE;
        for (GroupState gs : ts.groups.values()) {
            hwm = Math.min(hwm, gs.committed);
        }
        int removed = 0;
        while (!ts.messages.isEmpty() && ts.messages.peekFirst().offset() < hwm) {
            Message m = ts.messages.pollFirst();
            ts.baseOffset = m.offset() + 1;
            removed++;
        }
        if (removed > 0) {
            log.debug("回收已被所有消费组越过的消息前缀: topic={}, 删除{}条, 新baseOffset={}",
                    ts.name, removed, ts.baseOffset);
        }
    }

    /** 判断偏移量在该组是否已处于终态（已确认折叠或在乱序完成集合/死信集合中）。 */
    private boolean isCompleted(GroupState gs, long offset) {
        return offset < gs.committed || gs.completed.contains(offset);
    }

    /** 标记完成并折叠连续位点，随后尝试回收存储。 */
    private void complete(TopicState ts, GroupState gs, long offset) {
        gs.completed.add(offset);
        foldCommitted(gs);
        evictConsumedPrefix(ts);
    }

    /** 把乱序完成集合中的连续前缀折叠进 committed（位点只进不退）。 */
    private void foldCommitted(GroupState gs) {
        while (gs.completed.remove(gs.committed)) {
            gs.deliveryCounts.remove(gs.committed);
            gs.committed++;
        }
    }

    /** 返回组内下一条可投递偏移量；没有则 -1。 */
    private long nextDeliverableOffset(TopicState ts, GroupState gs) {
        for (Message m : ts.messages) {
            long off = m.offset();
            if (off < gs.committed) {
                continue;
            }
            if (gs.completed.contains(off) || gs.inflight.containsKey(off)) {
                continue;
            }
            return off;
        }
        return -1;
    }

    /** 按偏移量取保留中的消息；已被回收则为 null。 */
    private Message findMessage(TopicState ts, long offset) {
        if (offset < ts.baseOffset) {
            return null;
        }
        for (Message m : ts.messages) {
            if (m.offset() == offset) {
                return m;
            }
            if (m.offset() > offset) {
                break;
            }
        }
        return null;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
