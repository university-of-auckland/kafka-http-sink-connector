package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import nz.ac.auckland.kafka.http.sink.handler.ExceptionHandler;
import nz.ac.auckland.kafka.http.sink.handler.ExceptionStrategyHandlerFactory;
import nz.ac.auckland.kafka.http.sink.handler.StopTaskHandler;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import nz.ac.auckland.kafka.http.sink.util.TraceIdGenerator;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;

public class ApiRequestInvoker {

    private final RequestBuilder requestBuilder;
    private final HttpSinkConnectorConfig config;
    private final SinkTaskContext sinkContext;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private ExceptionHandler exceptionHandler;

    public ApiRequestInvoker(final HttpSinkConnectorConfig config, final SinkTaskContext context) {
        log.debug("Initializing ApiRequestInvoker");
        this.config = config;
        this.sinkContext = context;
        this.requestBuilder = new ApiRequestBuilder();
        setExceptionStrategy();
    }

    ApiRequestInvoker(final HttpSinkConnectorConfig config,
                      final SinkTaskContext context, RequestBuilder requestBuilder) {
        this.config = config;
        this.sinkContext = context;
        this.requestBuilder = requestBuilder;
        setExceptionStrategy();
    }

    public void invoke(final Collection<SinkRecord> records, String traceId){
        for(SinkRecord record: records){
            String spanHash = this.sinkContext.configs().get("name") + record.topic() + record.kafkaPartition() + record.kafkaOffset();
            String spanId = TraceIdGenerator.generateTraceId(spanHash);
            MDC.put("X-B3-TraceId",traceId);
            MDC.put("X-B3-SpanId",spanId);
            MDC.put("X-B3-Info", buildLogInfo(record));
            log.info("Processing record: topic={}  partition={} offset={} value={}", record.topic(), record.kafkaPartition(), record.kafkaOffset(), record.value().toString());
            sendAPiRequest(record , spanId);
            MDC.clear();
        }
    }

    private String buildLogInfo(SinkRecord record) {
        return "connection=" +
                this.sinkContext.configs().get("name") +
                "|kafka_topic=" +
                record.topic() +
                "|kafka_partition=" +
                record.kafkaPartition() +
                "|kafka_offset=" +
                record.kafkaOffset();
    }

    private void sendAPiRequest(SinkRecord record, String spanId){
        KafkaRecord kafkaRecord = new KafkaRecord(record);
        try {
            requestBuilder.createRequest(config,kafkaRecord)
                         .setHeaders(config.headers, spanId, config.headerSeparator)
                         .sendPayload(record.value().toString());
        }catch (ApiResponseErrorException e) {
            exceptionHandler.handel(e);
        }catch (ApiRequestErrorException e){
            new StopTaskHandler().handel(e);
        }
    }

    private void setExceptionStrategy() {
        exceptionHandler = ExceptionStrategyHandlerFactory.getInstance(config, sinkContext);
    }
}
