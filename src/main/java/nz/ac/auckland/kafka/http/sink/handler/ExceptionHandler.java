package nz.ac.auckland.kafka.http.sink.handler;


import nz.ac.auckland.kafka.http.sink.request.CallBackApiException;

public interface ExceptionHandler {

    void handel(CallBackApiException e);
}
