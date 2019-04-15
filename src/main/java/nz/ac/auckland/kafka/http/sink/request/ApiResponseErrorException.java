package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;

public class ApiResponseErrorException extends CallBackApiException {
    KafkaRecord record;
    public ApiResponseErrorException(String msg) {
        super(msg);
    }

    public ApiResponseErrorException(String msg, KafkaRecord record) {
        super(msg);
        this.record = record;
    }

    public KafkaRecord getRecord(){
        return record;
    }
}
