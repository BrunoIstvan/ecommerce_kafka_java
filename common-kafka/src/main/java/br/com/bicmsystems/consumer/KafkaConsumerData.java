package br.com.bicmsystems.consumer;

import java.util.regex.Pattern;

public class KafkaConsumerData {
    private final String groupId;
    private final String topic;
    private final Pattern pattern;

    public KafkaConsumerData(String groupId, String topic, Pattern pattern) {
        this.groupId = groupId;
        this.topic = topic;
        this.pattern = pattern;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTopic() {
        return topic;
    }

    public Pattern getPattern() {
        return pattern;
    }
}
