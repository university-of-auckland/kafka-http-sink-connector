package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;

public class CallBackApiException extends RuntimeException {
    public CallBackApiException(String msg) {
        super(msg);
    }
    public KafkaRecord getRecord(){
        return null;
    }
}
