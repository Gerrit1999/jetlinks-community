package org.jetlinks.core.message.codec;

import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.common.message.MessageExt;

import javax.annotation.Nonnull;

/**
 * @author Gerrit
 * @since 2023/6/8 14:45:07
 */
@Data
@AllArgsConstructor
public class RocketMQMessage implements EncodedMessage {

    private final MessageExt messageExt;
    private final ConsumeConcurrentlyContext context;

    @Nonnull
    @Override
    public ByteBuf getPayload() {
        // TODO
        throw new UnsupportedOperationException();
    }
}
