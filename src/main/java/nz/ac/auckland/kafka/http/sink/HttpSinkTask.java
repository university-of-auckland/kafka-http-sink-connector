/*
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nz.ac.auckland.kafka.http.sink;

import nz.ac.auckland.kafka.http.sink.request.ApiRequestInvoker;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class HttpSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(HttpSinkTask.class);

  HttpSinkConnectorConfig config;
  ApiRequestInvoker apiRequestInvoker;


  @Override
  public void start(final Map<String, String> props) {
    log.info("Starting task");
    config = new HttpSinkConnectorConfig(props);
    apiRequestInvoker = new ApiRequestInvoker(config, context);
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    log.info("Totals records:{}", records.size());
    if (records.isEmpty()) {
      return;
    }
    apiRequestInvoker.invoke(records);
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    // Not necessary
  }

  public void stop() {
    log.info("Stopping task");
  }

  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

}
