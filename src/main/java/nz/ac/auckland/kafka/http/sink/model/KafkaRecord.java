package nz.ac.auckland.kafka.http.sink.model;

import org.apache.kafka.connect.sink.SinkRecord;

public class KafkaRecord {

    private String topic;
    private String key;
    private long offset;
    private long partition;
    private String value;

    public KafkaRecord(SinkRecord record) {
        this.topic = record.topic();
        this.key = record.key()== null ? "" : record.key().toString();
        this.offset = record.kafkaOffset();
        this.partition = record.kafkaPartition();
        this.value = record.value().toString();
    }

    public String getTopic() {
        return topic;
    }

    public long getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    @Override
    public String toString() {
        return "KafkaRecord{" +
                "topic='" + topic + '\'' +
                ", key='" + key + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", value='" + value + '\'' +
                '}';
    }
}
