package nz.ac.auckland.kafka.http.sink.handler;

import nz.ac.auckland.kafka.http.sink.request.CallBackApiException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;

public class DeadLetterQueueHandler implements ExceptionHandler {
    private final ErrantRecordReporter errantRecordReporter;

    public DeadLetterQueueHandler(ErrantRecordReporter errantRecordReporter) {

        this.errantRecordReporter = errantRecordReporter;
    }

    @Override
    public void handle(CallBackApiException e) {
        this.errantRecordReporter.report(e.getRecord().getRecord(), e);
    }
}
