package nz.ac.auckland.kafka.http.sink.handler;

import nz.ac.auckland.kafka.http.sink.request.CallBackApiException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DropMessageStrategyHandler implements ExceptionHandler {
    private final SinkTaskContext sinkContext;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public DropMessageStrategyHandler(SinkTaskContext context) {
        log.info("Exception strategy: Drop message Strategy.");
        this.sinkContext = context;
    }

    @Override
    public void handel(CallBackApiException e) {
        log.warn("Drop message Strategy: Dropping message {}" , e.getRecord());
        sinkContext.requestCommit();
    }
}
