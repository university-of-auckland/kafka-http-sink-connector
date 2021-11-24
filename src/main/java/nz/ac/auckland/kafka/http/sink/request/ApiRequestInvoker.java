package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import nz.ac.auckland.kafka.http.sink.handler.ExceptionHandler;
import nz.ac.auckland.kafka.http.sink.handler.RequestExceptionStrategyHandlerFactory;
import nz.ac.auckland.kafka.http.sink.handler.ResponseExceptionStrategyHandlerFactory;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import nz.ac.auckland.kafka.http.sink.util.TraceIdGenerator;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.Date;
import java.util.Optional;

import java.lang.InterruptedException;

public class ApiRequestInvoker {

    private final RequestBuilder requestBuilder;
    private final HttpSinkConnectorConfig config;
    private final SinkTaskContext sinkContext;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private ExceptionHandler responseExceptionHandler;
    private ExceptionHandler requestExceptionHandler;

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

    public void invoke(final Collection<SinkRecord> records){
        for(SinkRecord record: records){

            long start = System.currentTimeMillis();

            String spanHash = this.sinkContext.configs().get("name") + record.topic() + record.kafkaPartition() + record.kafkaOffset();

            String traceId = this.getHeader(record, ApiRequest.REQUEST_HEADER_TRACE_ID_KEY)
                                 .orElseGet(() -> TraceIdGenerator.generateTraceId(spanHash));

            String kafkaDetails = buildLogInfo(record);

            MDC.put(ApiRequest.REQUEST_HEADER_TRACE_ID_KEY, traceId);
            MDC.put(ApiRequest.REQUEST_HEADER_SPAN_ID_KEY, traceId);
            MDC.put(ApiRequest.REQUEST_HEADER_INFO_KEY, kafkaDetails);

            log.info("Processing record: topic={}  partition={} offset={} value={}", record.topic(), record.kafkaPartition(), record.kafkaOffset(), record.value().toString());
            
            Long messageDelay = this.getMessageDelay(record, start);

            if (messageDelay == 0L){
                sendAPiRequest(record , traceId);
            }else {
                sendDelayedAPiRequest(record, traceId, messageDelay);
            }

            long executionTime = System.currentTimeMillis() - start;
            log.debug("Metrics=Latency metricSystem=kafka-connector-{} metricMeasure=single-record-processing-time metricValue={} kafkaDetails={}",
                    sinkContext.configs().get("name"), executionTime, kafkaDetails);

            MDC.clear();
        }
    }

    private Optional<String> getHeader(SinkRecord record, String headerName) {

        Header header = record.headers().lastWithName(headerName);
        if(header != null) {

            Object value = header.value();
            if(value != null) {
                log.debug("Found header '{}' value '{}'", headerName, value);
                return Optional.of(String.valueOf(value));
            }
        }

        return Optional.empty();
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
            //Reset the handler retryIndex to zero
            responseExceptionHandler.reset();
            requestExceptionHandler.reset();
        }catch (ApiResponseErrorException e) {
            responseExceptionHandler.handel(e);
        }catch (ApiRequestErrorException e){
            requestExceptionHandler.handel(e);
        }
    }

    private void setExceptionStrategy() {
        requestExceptionHandler = RequestExceptionStrategyHandlerFactory
                .getInstance(RequestExceptionStrategyHandlerFactory.ExceptionStrategy.PROGRESS_BACK_OFF_STOP_TASK,
                        config, sinkContext);
        responseExceptionHandler = ResponseExceptionStrategyHandlerFactory
                .getInstance(config.exceptionStrategy, config, sinkContext);
    }

    private Long getMessageDelay(SinkRecord record, long start) {
        long delay = this.config.requestDelay * 1000L;
      
        if (record.timestampType().equals(TimestampType.NO_TIMESTAMP_TYPE)){
          log.info("Checking delay: now={}, sleep={}", start, new Date(start).toString(), new Date(delay).toString());
          // If there is no timestamp we sleep for the specified delay
          return delay;
        }
      
        Long timePassed = start - record.timestamp();
        Long delayTimePassed = delay - timePassed;
      
        log.info("Checking delay: now={}, record-timestamp={}, timepassed={}, sleep={}", new Date(start).toString(), new Date(record.timestamp()).toString(), timePassed, delayTimePassed);
        
        return timePassed < delay ? delayTimePassed : 0L;
    }

    private void sendDelayedAPiRequest(SinkRecord record, String spanId, long delay){
        try {
            Thread.sleep(delay);
            this.sendAPiRequest(record, spanId);
        } catch (InterruptedException e){
            log.error("The delay for the record was interupted. The record has not been forwarded");
            e.printStackTrace();
        }
    }

}
