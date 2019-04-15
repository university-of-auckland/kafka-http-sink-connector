package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;

public class ApiRequest {

    private static final String REQUEST_HEADER_CORRELATION_ID_KEY = "X-API-Correlation-Id";
    private HttpURLConnection connection;
    private final static String STREAM_ENCODING = "UTF-8";
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private KafkaRecord kafkaRecord;


    ApiRequest(HttpURLConnection connection, KafkaRecord kafkaRecord) {
        this.connection = connection;
        this.kafkaRecord = kafkaRecord;
    }

    public ApiRequest setHeaders(String headers, String headerSeparator) {
        log.info("Processing headers: headerSeparator={}", headerSeparator);
        for (String headerKeyValue : headers.split(headerSeparator)) {
            if (headerKeyValue.contains(":")) {
                String key = headerKeyValue.split(":")[0];
                String value = headerKeyValue.split(":")[1];
                log.info("Setting header property: {}",key);
                connection.setRequestProperty(key, value);
            }
        }
        addCorrelationIdHeader();
        return this;
    }

    private void addCorrelationIdHeader() {
        String correlationId = kafkaRecord.getTopic() + "-" + kafkaRecord.getOffset();
        log.info("Adding correlationId header: {} = {} ",REQUEST_HEADER_CORRELATION_ID_KEY, correlationId);
        connection.setRequestProperty(REQUEST_HEADER_CORRELATION_ID_KEY, correlationId);
    }

    public void sendPayload(String payload) {
        try(OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), STREAM_ENCODING)){
            writer.write(payload);
            writer.flush();
            writer.close();
            log.info("Submitted request: url={} payload={}",connection.getURL(), payload);
        } catch (Exception e) {
            throw new ApiRequestErrorException(e.getLocalizedMessage(), kafkaRecord);
        }

        validateResponse();
    }

    private void validateResponse() {
        try {
                JSONObject response = new JSONObject(getResponse());
                boolean retry = response.getBoolean("retry");
                connection.disconnect();
                if (retry) {
                    throw new ApiResponseErrorException("Unable to process message.", kafkaRecord);
                }
        }catch (Exception e) {
            throw new ApiResponseErrorException(e.getLocalizedMessage(), kafkaRecord);
        }
    }

    private String getResponse() throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream(),STREAM_ENCODING));
        StringBuilder sb = new StringBuilder();
        String line;
        while((line = br.readLine()) != null){
            sb.append(line);
        }
        br.close();
        String response = sb.toString();
        log.info("Response:{}", response);
        return response;
    }
}
