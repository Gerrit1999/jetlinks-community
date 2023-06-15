package org.jetlinks.community.network.rocketmq.client;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.jetlinks.community.network.DefaultNetworkType;
import org.jetlinks.community.network.Network;
import org.jetlinks.community.network.NetworkType;
import org.jetlinks.core.message.codec.RocketMQMessage;
import org.jetlinks.core.route.MqRoute;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gerrit
 * @since 2023/6/8 11:37:25
 */
@Slf4j
public class RocketMQClient implements Network {

    @Getter
    private final String id;
    @Getter
    private final String ip;
    @Getter
    private final Integer port;

    private final List<DefaultMQPushConsumer> consumers = new ArrayList<>();

    public RocketMQClient(String id, String ip, Integer port) {
        this.id = id;
        this.ip = ip;
        this.port = port;
    }

    @Override
    public NetworkType getType() {
        return DefaultNetworkType.ROCKETMQ_CLIENT;
    }

    @Override
    public void shutdown() {
        consumers.forEach(DefaultMQPushConsumer::shutdown);
    }

    @Override
    public boolean isAlive() {
        return true;
    }

    @Override
    public boolean isAutoReload() {
        return true;
    }

    /**
     * 同一NameServer, 同一消费者组, 不同topic的消息必须由同一个consumer对象来订阅
     * 或者设置不同的消费者组
     * 否则可能导致消息延迟甚至消息丢失等问题
     */
    public Flux<RocketMQMessage> subscribe(List<MqRoute> routes, String group) {
        return Flux.create(sink -> {
            try {
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
                consumer.setNamesrvAddr(ip + ":" + port);
                // 订阅一个或多个topic，并指定tag过滤条件，这里指定*表示接收所有tag的消息
                for (MqRoute route : routes) {
                    String topic = route.getTopic();
                    String subExpression = route.getSubExpression();
                    consumer.subscribe(topic, subExpression);
                }
                // 注册回调接口来处理从Broker中收到的消息
                consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                    // 发送消息列表和消费上下文到 Flux
                    for (MessageExt msg : msgs) {
                        sink.next(new RocketMQMessage(msg, context));
                    }
                    // 返回消息消费状态，ConsumeConcurrentlyStatus.CONSUME_SUCCESS为消费成功
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                });
                // 启动Consumer
                consumer.start();
                consumers.add(consumer);
                // 在取消订阅时停止 Consumer
                sink.onDispose(() -> {
                    consumer.shutdown();
                    log.info("Consumer Shutdown.");
                });
                log.info("Consumer Started.");
            } catch (Exception e) {
                // 发生异常时发送错误信号到 Flux
                sink.error(e);
            }
        });
    }
}
