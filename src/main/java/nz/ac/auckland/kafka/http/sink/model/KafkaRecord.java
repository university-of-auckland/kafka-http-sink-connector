package nz.ac.auckland.kafka.http.sink.model;

import org.apache.kafka.connect.sink.SinkRecord;

public class KafkaRecord {

    private final SinkRecord record;


    public KafkaRecord(SinkRecord record) {

        this.record = record;
    }

    public String getTopic() {
        return record.topic();
    }

    public long getPartition() {
        return record.kafkaPartition();
    }

    public long getOffset() {
        return record.kafkaOffset();
    }

    @Override
    public String toString() {
        return "KafkaRecord{" +
                "topic='" + record.topic() + '\'' +
                ", key='" + record.key() + '\'' +
                ", partition=" + record.kafkaPartition() +
                ", offset=" + record.kafkaOffset() +
                ", value='" + record.value() + '\'' +
                '}';
    }

    public SinkRecord getRecord() {
        return record;
    }
}
