package nz.ac.auckland.kafka.http.sink;

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

  private HttpSinkConnectorConfig config;
  private ApiRequestInvoker apiRequestInvoker;

  @Override
  public void start(final Map<String, String> props) {
    String connectorName = context.configs().get("name");
    MDC.put("X-B3-TraceId","-");
    MDC.put("X-B3-SpanId","-");
    MDC.put("X-B3-Info", "connection=" + connectorName);
    log.info("Starting task for {} ", connectorName);
    config = new HttpSinkConnectorConfig(props);
    apiRequestInvoker = new ApiRequestInvoker(config, context);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.debug("Totals records:{}", records.size());
    if (records.isEmpty()) {
      return;
    }
    apiRequestInvoker.invoke(records);

    //Request a commit of the processed message
    //else the commit is triggered after the  'offset.flush.interval.ms'
    context.requestCommit();
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    MDC.clear();
  }

  public void stop() {
    MDC.put("X-B3-TraceId","-");
    MDC.put("X-B3-SpanId","-");
    MDC.put("X-B3-Info", "connection=" + context.configs().get("name"));
    log.info("Stopping task for {}", context.configs().get("name"));
    MDC.clear();
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

}
