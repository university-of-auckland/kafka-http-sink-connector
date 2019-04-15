package nz.ac.auckland.kafka.http.sink;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class HttpSinkConnector extends SinkConnector {

  private static final Logger log = LoggerFactory.getLogger(HttpSinkConnector.class);

  private Map<String, String> configProps;

  public Class<? extends Task> taskClass() {
    return HttpSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProps);
    }
    return configs;
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = props;
  }

  @Override
  public void stop() { }

  @Override
  public ConfigDef config() {
    return HttpSinkConnectorConfig.conf();
  }


  @Override
  public String version() {
    return Version.getVersion();
  }
}
