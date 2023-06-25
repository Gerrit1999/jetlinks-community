package com.demo;

import org.apache.rocketmq.common.message.MessageExt;
import org.jetlinks.core.message.DeviceLogMessage;
import org.jetlinks.core.message.Message;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.message.codec.DeviceMessageCodec;
import org.jetlinks.core.message.codec.MessageDecodeContext;
import org.jetlinks.core.message.codec.MessageEncodeContext;
import org.jetlinks.core.message.codec.RocketMQMessage;
import org.jetlinks.core.message.codec.Transport;
import reactor.core.publisher.Mono;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Gerrit
 * @since 2023/6/8 16:26:12
 */
public class MyRocketMQDeviceMessageCodec implements DeviceMessageCodec {

    private final Transport transport;


    public MyRocketMQDeviceMessageCodec(Transport transport) {
        this.transport = transport;
    }

    public MyRocketMQDeviceMessageCodec() {
        this(DefaultTransport.MQ);
    }

    @Override
    public Transport getSupportTransport() {
        return transport;
    }

    /**
     * 协议仅用于从RocketMQ订阅消息, 不需要编码
     */
    @Nonnull
    public Mono<RocketMQMessage> encode(@Nonnull MessageEncodeContext context) {
        return Mono.defer(Mono::empty);
    }

    @Nonnull
    @Override
    public Mono<Message> decode(@Nonnull MessageDecodeContext context) {
        return Mono.fromSupplier(() -> {
            RocketMQMessage message = (RocketMQMessage) context.getMessage();
            MessageExt messageExt = message.getMessageExt();
            String topic = messageExt.getTopic();
            String tags = messageExt.getTags();
            byte[] body = messageExt.getBody();
            Map<String, Object> map = new HashMap<>();
            map.put("topic", topic);
            map.put("tags", tags);
            // 将mq消息转为设备日志消息
            DeviceLogMessage deviceLogMessage = new DeviceLogMessage();
            deviceLogMessage.setHeaders(map);
            deviceLogMessage.setLog(new String(body));
            return deviceLogMessage;
        });
    }
}
