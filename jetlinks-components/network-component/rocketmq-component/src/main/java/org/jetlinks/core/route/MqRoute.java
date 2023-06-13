package org.jetlinks.core.route;

public interface MqRoute extends Route {

    static Builder builder(String topic, String tag) {
        return DefaultMqRoute
            .builder()
            .topic(topic)
            .tag(tag);
    }

    String getTopic();

    String getTag();

    @Override
    default String getAddress() {
        return getTopic();
    }

    @Override
    default String getGroup() {
        return null;
    }

    interface Builder {

        Builder topic(String topic);

        Builder tag(String tag);

        Builder description(String description);

        Builder example(String example);

        MqRoute build();
    }
}
