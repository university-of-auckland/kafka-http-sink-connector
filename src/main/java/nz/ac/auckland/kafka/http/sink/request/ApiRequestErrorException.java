package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;

public class ApiRequestErrorException extends CallBackApiException {
    KafkaRecord record;
    public ApiRequestErrorException(String msg) {
        super(msg);
    }

    public ApiRequestErrorException(String msg, KafkaRecord record) {
        super(msg);
        this.record = record;
    }

    public KafkaRecord getRecord(){
        return record;
    }
}
