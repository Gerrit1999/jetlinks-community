package org.jetlinks.community.network.rocketmq.gateway.device;

import com.demo.MyProtocolSupportProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.jetlinks.community.network.rocketmq.client.RocketMQClient;
import org.jetlinks.community.protocol.SpringServiceContext;
import org.jetlinks.core.ProtocolSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import javax.annotation.Resource;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
@ExtendWith(SpringExtension.class)
class RocketMQClientDeviceGatewayTest {

    @Resource
    private ApplicationContext context;

    @Test
    void doStartup() throws InterruptedException {
        // 创建RocketMQ客户端
        RocketMQClient client = new RocketMQClient("rocketmq_client_0", "127.0.0.1", 9876);

        // 创建协议包
        SpringServiceContext serviceContext = new SpringServiceContext(context);
        MyProtocolSupportProvider protocolSupportProvider = new MyProtocolSupportProvider();
        Mono<ProtocolSupport> protocolSupportMono = protocolSupportProvider.create(serviceContext);

        // 监听10个设备网关的消息
        Flux.range(0, 10)
            .flatMap(i -> Mono.defer(() -> {
                RocketMQClientDeviceGateway gateway = new RocketMQClientDeviceGateway("rocketmq_gateway_" + i, client);
                gateway.setProtocol(protocolSupportMono);
                return gateway.doStartup()
                    .subscribeOn(Schedulers.parallel())
                    .doOnSuccess(nil -> log.info("{} startup success", gateway.getId()))
                    .doOnError(e -> log.error("{} startup error", gateway.getId(), e));
            }))
            .subscribe();

        Thread.currentThread().join();
    }

    @Test
    void send() throws Exception {
        // 初始化一个producer并设置Producer group name
        DefaultMQProducer producer = new DefaultMQProducer("product_group_name");
        // 设置NameServer地址
        producer.setNamesrvAddr("127.0.0.1:9876");
        // 启动producer
        producer.start();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        for (int i = 0; i < 1; i++) {
            // 创建一条消息，并指定topic、tag、body等信息，tag可以理解成标签，对消息进行再归类，RocketMQ可以在消费端对tag进行过滤
            Message msg = new Message("TopicTest" /* Topic */,
                "TagA" /* Tag */,
                ("Hello RocketMQ " + sdf.format(new Date())).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            // 利用producer进行发送，并同步等待发送结果
            SendResult sendResult = producer.send(msg);
            log.info("{}", sendResult);
        }
        // 一旦producer不再使用，关闭producer
        producer.shutdown();
    }
}