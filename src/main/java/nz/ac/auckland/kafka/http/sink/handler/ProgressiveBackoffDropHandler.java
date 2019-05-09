package nz.ac.auckland.kafka.http.sink.handler;

import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import nz.ac.auckland.kafka.http.sink.request.CallBackApiException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class ProgressiveBackoffDropHandler implements ExceptionHandler {
    private static final int ADJUST_ZERO_ELEMENT = 1;
    private static final long MILLI_SEC_MULTIPLIER = 1000;
    private int retryIndex = 0;
    private int maxRetries;
    private String[] retryBackoffsec;
    private final SinkTaskContext sinkContext;
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    ProgressiveBackoffDropHandler(HttpSinkConnectorConfig config, SinkTaskContext context) {
        log.info("Exception strategy: Progressive back-off drop Strategy retries={}.", Arrays.toString(config.retryBackoffsec));
        this.maxRetries = config.retryBackoffsec.length;
        this.retryBackoffsec = config.retryBackoffsec;
        this.sinkContext = context;
    }

    @Override
    public void handel(CallBackApiException e) {
        if (retryIndex >= maxRetries) {
            log.error("Progressive back-off drop Strategy: Dropping the message {} after {} retries.",e.getRecord(),maxRetries);
            retryIndex = 0;
            sinkContext.requestCommit();
        } else {
            long waitTime = Long.parseLong(retryBackoffsec[retryIndex]) * MILLI_SEC_MULTIPLIER;
            log.info("Progressive back-off drop Strategy: {}/{} Will Retry after {} ms. Brackets:{} secs.", retryIndex + ADJUST_ZERO_ELEMENT, maxRetries,waitTime, retryBackoffsec);
            sinkContext.timeout(waitTime);
            retryIndex++;
            throw new RetriableException(e);
        }
    }
}
