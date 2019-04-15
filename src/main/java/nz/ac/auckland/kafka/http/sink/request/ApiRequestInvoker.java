package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import nz.ac.auckland.kafka.http.sink.handler.ExceptionHandler;
import nz.ac.auckland.kafka.http.sink.handler.ExceptionStrategyHandlerFactory;
import nz.ac.auckland.kafka.http.sink.handler.StopTaskStrategyHandler;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class ApiRequestInvoker {

    private final ApiRequestBuilder apiRequestBuilder = new ApiRequestBuilder();
    private final HttpSinkConnectorConfig config;
    private final SinkTaskContext sinkContext;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private ExceptionHandler exceptionHandler;

    public ApiRequestInvoker(final HttpSinkConnectorConfig config, final SinkTaskContext context) {
        this.config = config;
        this.sinkContext = context;
        setExceptionStrategy();
    }

    public void invoke(final Collection<SinkRecord> records){
        for(SinkRecord record: records){
            log.info("Processing record: topic={}  offset={} value={}", record.topic(), record.kafkaOffset(), record.value().toString());
            sendAPiRequest(record);
        }
    }

    private void sendAPiRequest(SinkRecord record){
        KafkaRecord kafkaRecord = new KafkaRecord(record);
        try {
            apiRequestBuilder.createRequest(config.httpApiUrl, config.requestMethod.toString(),kafkaRecord)
                         .setHeaders(config.headers, config.headerSeparator)
                         .sendPayload(record.value().toString());
        }catch (ApiResponseErrorException e) {
            exceptionHandler.handel(e);
        }catch (ApiRequestErrorException e){
            new StopTaskStrategyHandler().handel(e);
        }
    }

    private void setExceptionStrategy() {
        exceptionHandler = ExceptionStrategyHandlerFactory.getInstance(config, sinkContext);
    }
}
