package nz.ac.auckland.kafka.http.sink.handler;


import nz.ac.auckland.kafka.http.sink.request.CallBackApiException;

public interface ExceptionHandler {

    void handel(CallBackApiException e);

    /**
     * Reset the handlers retry index if present
     * In-case the message was processed successfully in one of the retry
     * the next message retry should not start from previously incremented index
     */
    default void reset(){}
}
