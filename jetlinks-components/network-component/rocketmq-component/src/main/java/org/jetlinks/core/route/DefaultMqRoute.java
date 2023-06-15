package org.jetlinks.core.route;

import lombok.Getter;

@Getter
class DefaultMqRoute implements MqRoute {

    private final String topic;
    private final String subExpression;
    private final String description;
    private final String example;

    DefaultMqRoute(String topic, String subExpression, String description, String example) {
        this.topic = topic;
        this.subExpression = subExpression;
        this.description = description;
        this.example = example;
    }

    static DefaultMqRouteBuilder builder() {
        return new DefaultMqRouteBuilder();
    }

    static class DefaultMqRouteBuilder implements Builder {
        private String topic;
        private String tag;
        private String description;
        private String example;

        DefaultMqRouteBuilder() {
        }

        public DefaultMqRouteBuilder topic(String topic) {
            this.topic = topic;
            return this;
        }

        public DefaultMqRouteBuilder tag(String tag) {
            this.tag = tag;
            return this;
        }

        public DefaultMqRouteBuilder description(String description) {
            this.description = description;
            return this;
        }

        public DefaultMqRouteBuilder example(String example) {
            this.example = example;
            return this;
        }

        public DefaultMqRoute build() {
            return new DefaultMqRoute(topic, tag, description, example);
        }

        public String toString() {
            return "DefaultMqRoute.DefaultMqRouteBuilder(topic=" + this.topic + ", tag=" + this.tag + ", description=" + this.description + ", example=" + this.example + ")";
        }
    }
}
