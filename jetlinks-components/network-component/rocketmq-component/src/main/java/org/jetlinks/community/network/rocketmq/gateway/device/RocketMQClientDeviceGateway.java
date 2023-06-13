package org.jetlinks.community.network.rocketmq.gateway.device;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.hswebframework.web.logger.ReactiveLogger;
import org.jetlinks.community.gateway.AbstractDeviceGateway;
import org.jetlinks.community.network.rocketmq.client.RocketMQClient;
import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.DeviceMessageCodec;
import org.jetlinks.core.message.codec.FromDeviceMessageContext;
import org.jetlinks.core.route.MqRoute;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.Objects;

/**
 * @author Gerrit
 * @since 2023/6/8 10:53:58
 */
@Slf4j
public class RocketMQClientDeviceGateway extends AbstractDeviceGateway {

    private final RocketMQClient client;

    @Getter
    private Mono<ProtocolSupport> protocol;

    @Getter
    private Mono<DeviceMessageCodec> codecMono;

    public RocketMQClientDeviceGateway(String id, RocketMQClient client) {
        super(id);
        this.client = client;
    }

    public void setProtocol(Mono<ProtocolSupport> protocol) {
        this.protocol = Objects.requireNonNull(protocol, "protocol");
        this.codecMono = protocol.flatMap(p -> p.getMessageCodec(DefaultTransport.MQ));
    }

    @Override
    protected Mono<Void> doShutdown() {
        return Mono.fromRunnable(client::shutdown);
    }

    @Override
    protected Mono<Void> doStartup() {
        return reload();
    }

    protected Mono<Void> reload() {
        return getProtocol()
            .flatMap(support -> support
                .getRoutes(DefaultTransport.MQ)
                .filter(MqRoute.class::isInstance)
                .cast(MqRoute.class)
                .collectList()
                .doOnEach(ReactiveLogger.onNext(routes -> {
                    // 协议包里没有配置Mq Topic信息
                    if (CollectionUtils.isEmpty(routes)) {
                        log.warn("The protocol [{}] is not configured with topics information", support.getId());
                    }
                }))
                .doOnNext(routes -> doSubscribe(routes, String.format("%s-%s", client.getId(), getId())))
            )
            .then();
    }

    protected void doSubscribe(List<MqRoute> mqRoutes, String group) {
        client.subscribe(mqRoutes, group)
            .flatMap(message -> codecMono   // 将每个 RocketMQ 消息解码为消息流
                .flatMapMany(codec -> codec.decode(FromDeviceMessageContext.of(null, message)))
                .flatMap(msg -> {
                    log.info("{} receive message: {}", group, msg);
                    return Mono.empty();
                })
                .subscribeOn(Schedulers.parallel()) // 在并行调度器上执行处理
                .onErrorResume(err -> {
                    log.error("handle rocketmq client message error:{}", message, err);
                    return Mono.empty();
                }), Integer.MAX_VALUE)
            .subscribe();
    }
}