package nz.ac.auckland.kafka.http.sink.handler;

import nz.ac.auckland.kafka.http.sink.request.CallBackApiException;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StopTaskStrategyHandler implements ExceptionHandler{
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public StopTaskStrategyHandler() {
        log.info("Exception strategy: Stop task strategy.");
    }

    @Override
    public void handel(CallBackApiException e) {
        log.error("Stop task Strategy: Stopping task. \nError:{} \nErrored record: {}" , e.getMessage(),e.getRecord());
        throw new ConnectException(e);
    }
}
