package nz.ac.auckland.kafka.http.sink;

import nz.ac.auckland.kafka.http.sink.request.ApiRequest;
import nz.ac.auckland.kafka.http.sink.request.ApiRequestInvoker;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.Map;

public class HttpSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

  private ApiRequestInvoker apiRequestInvoker;
  private String connectorName;

  @Override
  public void start(final Map<String, String> props) {

    connectorName = context.configs().get("name");
    MDC.put(ApiRequest.REQUEST_HEADER_TRACE_ID_KEY,"-");
    MDC.put(ApiRequest.REQUEST_HEADER_SPAN_ID_KEY,"-");
    MDC.put(ApiRequest.REQUEST_HEADER_INFO_KEY, "connection=" + connectorName);

    log.info("Starting task for {} ", connectorName);

    HttpSinkConnectorConfig config = new HttpSinkConnectorConfig(props);
    apiRequestInvoker = new ApiRequestInvoker(config, context);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    long start = System.currentTimeMillis();

    int batchSize = records.size();
    log.debug("Totals records:{}", batchSize);
    if (records.isEmpty()) {
      return;
    }
    apiRequestInvoker.invoke(records);

    //Request a commit of the processed message
    //else the commit is triggered after the  'offset.flush.interval.ms'
    context.requestCommit();

    long executionTime = System.currentTimeMillis() - start;
    log.info("Metrics=Latency metricSystem=kafka-connector-{} metricMeasure=batch-processing-time metricValue={} batchSize={}", connectorName, executionTime, batchSize);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    MDC.clear();
  }



  public void stop() {
    MDC.put(ApiRequest.REQUEST_HEADER_TRACE_ID_KEY,"-");
    MDC.put(ApiRequest.REQUEST_HEADER_SPAN_ID_KEY,"-");
    MDC.put(ApiRequest.REQUEST_HEADER_INFO_KEY, "connection=" + context.configs().get("name"));
    log.info("Stopping task for {}", context.configs().get("name"));
    MDC.clear();
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

}
