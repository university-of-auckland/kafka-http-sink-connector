package nz.ac.auckland.kafka.http.sink.model;

import org.apache.kafka.connect.sink.SinkRecord;

public class KafkaRecord {

    private String topic;
    private String key;
    private long offset;
    private String value;

    public KafkaRecord(SinkRecord record) {
        this.topic = record.topic();
        this.key = record.key()== null ? "" : record.key().toString();
        this.offset = record.kafkaOffset();
        this.value = record.value().toString();
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "KafkaRecord{" +
                "topic='" + topic + '\'' +
                ", key='" + key + '\'' +
                ", offset=" + offset +
                ", value='" + value + '\'' +
                '}';
    }
}
