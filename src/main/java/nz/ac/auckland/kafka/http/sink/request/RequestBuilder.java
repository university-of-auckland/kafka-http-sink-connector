package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;

public interface RequestBuilder {

    Request createRequest(String uri, String httpMethod, KafkaRecord kafkaRecord);
}
