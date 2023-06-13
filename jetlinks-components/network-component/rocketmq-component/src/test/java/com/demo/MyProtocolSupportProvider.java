package com.demo;

import org.jetlinks.core.ProtocolSupport;
import org.jetlinks.core.defaults.CompositeProtocolSupport;
import org.jetlinks.core.message.codec.DefaultTransport;
import org.jetlinks.core.route.MqRoute;
import org.jetlinks.core.spi.ProtocolSupportProvider;
import org.jetlinks.core.spi.ServiceContext;
import org.jetlinks.supports.official.JetLinksDeviceMetadataCodec;
import reactor.core.publisher.Mono;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MyProtocolSupportProvider implements ProtocolSupportProvider {

    @Override
    public Mono<ProtocolSupport> create(ServiceContext context) {
        return Mono.defer(() -> {
            CompositeProtocolSupport support = new CompositeProtocolSupport();

            support.setId("jetlinks.v3.0");
            support.setName("JetLinks V3.0");
            support.setDescription("JetLinks Protocol Version 3.0");

            support.addRoutes(DefaultTransport.MQ, Stream.of("TopicTest", "TopicTest2")
                .map(topic -> MqRoute.builder(topic, "*").build())
                .collect(Collectors.toList())
            );
            support.setDocument(DefaultTransport.MQ,
                "document-rocketmq.md",
                MyProtocolSupportProvider.class.getClassLoader());

            support.setMetadataCodec(new JetLinksDeviceMetadataCodec());

            support.addMessageCodecSupport(new MyRocketMQDeviceMessageCodec());

            return Mono.just(support);
        });
    }
}
