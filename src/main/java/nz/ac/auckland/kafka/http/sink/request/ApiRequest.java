package nz.ac.auckland.kafka.http.sink.request;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;

import static nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig.HEADER_SEPERATOR_DEFAULT;

public class ApiRequest implements Request{

    static final String REQUEST_HEADER_CORRELATION_ID_KEY = "X-B3-TraceId";
    static final String REQUEST_HEADER_KAFKA_TOPIC_KEY = "X-Kafka-Topic";
    static final String REQUEST_HEADER_INFO_KEY = "X-B3-Info" ;
    private final static String STREAM_ENCODING = "UTF-8";
    private HttpURLConnection connection;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private KafkaRecord kafkaRecord;
    private static final List<Integer> CALLBACK_API_DOWN_HTTP_STATUS_CODE = Arrays.asList(502,503,504);


    ApiRequest(HttpURLConnection connection, KafkaRecord kafkaRecord) {
        this.connection = connection;
        this.kafkaRecord = kafkaRecord;
    }

    @Override
    public ApiRequest setHeaders(String headerString, String traceId) {
        if(HttpSinkConnectorConfig.nonJsonHeader) {
            setNonJsonHeaders(headerString);
        }else if(headerString != null && headerString.trim().length() > 0 ) {
            log.debug("Processing headers: {}", headerString);
            JsonObject headers = new JsonParser().parse(headerString).getAsJsonObject();

            log.debug("headers: {}", headers.toString());
            for (String headerKey : headers.keySet()) {
                log.debug("Setting header property: {}", headerKey);
                connection.setRequestProperty(headerKey, headers.get(headerKey).getAsString());
            }
        }
        addCorrelationIdHeader(traceId);
        addInfoHeader();
        addTopicHeader();
        return this;
    }

    private void setNonJsonHeaders(String headers){
        log.debug("Processing Non json headers: {}, separator: {} ", headers, HEADER_SEPERATOR_DEFAULT);
        for (String headerKeyValue : headers.split(HEADER_SEPERATOR_DEFAULT)) {
            if (headerKeyValue.contains(":")) {
                String key = headerKeyValue.split(":")[0];
                String value = headerKeyValue.split(":")[1];
                log.debug("Setting header property: {}",key);
                connection.setRequestProperty(key, value);
            }
        }
    }


    private void addTopicHeader() {
        log.debug("Adding topic header: {} = {} ",REQUEST_HEADER_KAFKA_TOPIC_KEY, kafkaRecord.getTopic());
        connection.setRequestProperty(REQUEST_HEADER_KAFKA_TOPIC_KEY, kafkaRecord.getTopic());
    }

    private void addCorrelationIdHeader(String traceId) {
        log.debug("Adding correlationId header: {} = {} ",REQUEST_HEADER_CORRELATION_ID_KEY, traceId);
        connection.setRequestProperty(REQUEST_HEADER_CORRELATION_ID_KEY,traceId );
    }
    private void addInfoHeader() {
        String info = "topic=" + kafkaRecord.getTopic() + "|partition=" + kafkaRecord.getPartition()
                + "|offset=" + kafkaRecord.getOffset();
        log.debug("Adding info header: {} = {} ",REQUEST_HEADER_INFO_KEY, info);
        connection.setRequestProperty(REQUEST_HEADER_INFO_KEY, info);
    }

    @Override
    public void sendPayload(String payload) {
        try(OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream(), STREAM_ENCODING)){
            writer.write(payload);
            writer.flush();
            writer.close();
            log.info("Submitted request: url={} payload={}",connection.getURL(), payload);
            isSendRequestSuccessful();
            validateResponse();
        }catch (ApiResponseErrorException e) {
            throw e;
        }catch (Exception e) {
            throw new ApiRequestErrorException(e.getLocalizedMessage(), kafkaRecord);
        } finally {
            connection.disconnect();
        }
    }

    private void isSendRequestSuccessful() {
        try {
            int statusCode = connection.getResponseCode();
            log.info("Response Status: {}", statusCode);
            if(CALLBACK_API_DOWN_HTTP_STATUS_CODE.contains(statusCode)){
                throw new ApiRequestErrorException("Unable to connect to callback API: "
                        + " received status: " + statusCode, kafkaRecord);
            }
        }catch (SocketTimeoutException e) {
            log.warn("Unable to obtain response from callback API. \n Error:{} ",e.getMessage());
            throw new ApiResponseErrorException(e.getLocalizedMessage());
        } catch (IOException e) {
            log.warn("Error checking if Send Request was Successful.");
            e.printStackTrace();
        }
    }

    private void validateResponse() {
        JsonObject response =  new JsonParser().parse(getResponse()).getAsJsonObject();

        boolean retry = response.get("retry").getAsBoolean();
        if (retry) {
            throw new ApiResponseErrorException("Unable to process message.", kafkaRecord);
        }
    }

    private String getResponse() {
        try {
            return readResponse(connection.getInputStream());
        } catch (IOException e) {
            try {
                String error = readResponse(connection.getErrorStream());
                log.error("Error Validating response. \n Error:{}", error);
                throw new ApiRequestErrorException(error, kafkaRecord);
            } catch (IOException e1) {
                log.error("Error Validating response. \n Error:{}", e1.getLocalizedMessage());
                throw new ApiRequestErrorException(e1.getLocalizedMessage(), kafkaRecord);
            }

        }
    }

    private String readResponse(InputStream stream) throws IOException {
        StringBuilder sb;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream, STREAM_ENCODING))) {
            sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            String response = sb.toString();
            log.info("Response:{}", response);
            return response;
        }
    }
}
