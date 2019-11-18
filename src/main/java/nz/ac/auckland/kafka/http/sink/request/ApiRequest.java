package nz.ac.auckland.kafka.http.sink.request;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.List;


public class ApiRequest implements Request{

    static final String REQUEST_HEADER_TRACE_ID_KEY = "X-B3-TraceId";
    static final String REQUEST_HEADER_SPAN_ID_KEY = "X-B3-SpanId";
    static final String REQUEST_HEADER_SAMPLED_KEY = "X-B3-Sampled";
    static final String REQUEST_HEADER_SAMPLED_VALUE = "Defer";

    static final String REQUEST_HEADER_KAFKA_TOPIC_KEY = "X-Kafka-Topic";
    static final String REQUEST_HEADER_INFO_KEY = "X-B3-Info" ;
    private final static String STREAM_ENCODING = "UTF-8";
    private HttpURLConnection connection;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private KafkaRecord kafkaRecord;
    private static final List<Integer> CALLBACK_API_DOWN_HTTP_STATUS_CODE = Arrays.asList(502,503,504,401,403,405,406);

    private enum ResponseTypes {ERROR , SUCCESS}


    ApiRequest(HttpURLConnection connection, KafkaRecord kafkaRecord) {
        this.connection = connection;
        this.kafkaRecord = kafkaRecord;
    }

    @Override
    public ApiRequest setHeaders(String headerString, String spanId, String separator) {

        try{
            if(headerString != null && headerString.trim().length() > 0 ) {
                JsonObject headers = new JsonParser().parse(headerString).getAsJsonObject();
                log.debug("Processing {} headers", headers.size());
                for (String headerKey : headers.keySet()) {
                    log.debug("Setting header property: {}", headerKey);
                    connection.setRequestProperty(headerKey, headers.get(headerKey).getAsString());
                }
            }
        }catch(JsonSyntaxException ex){
            setNonJsonHeaders(headerString, separator);
        }
        addB3Header(spanId);
        addInfoHeader();
        addTopicHeader();
        return this;
    }

    private void setNonJsonHeaders(String headers, String separator){
        log.debug("Processing Non json headers with separator: {} ", separator);
        for (String headerKeyValue : headers.split(separator)) {
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

    private void addB3Header(String traceId) {
        log.debug("Adding B3 headers");
        connection.setRequestProperty(REQUEST_HEADER_TRACE_ID_KEY,traceId );
        connection.setRequestProperty(REQUEST_HEADER_SPAN_ID_KEY,traceId );
        connection.setRequestProperty(REQUEST_HEADER_SAMPLED_KEY,REQUEST_HEADER_SAMPLED_VALUE );
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
            processResponse();
        }catch (ApiResponseErrorException e) {
            throw e;
        }catch (Exception e) {
            throw new ApiRequestErrorException(e.getLocalizedMessage(), kafkaRecord);
        } finally {
            connection.disconnect();
        }
    }

    private void processResponse() {
        try {
            int statusCode = connection.getResponseCode();
            log.info("Response Status: {}", statusCode);
            RetryIndicator retryIndicator = getResponseIndicator();
            log.info("Retry Indicator: {}", retryIndicator);
            if(retryIndicator == RetryIndicator.UNKNOWN
                    && CALLBACK_API_DOWN_HTTP_STATUS_CODE.contains(statusCode)){
                throw new ApiRequestErrorException("Unable to connect to callback API: "
                        + " received status: " + statusCode, kafkaRecord);
            } else if (retryIndicator.shouldRetry) {
                throw new ApiResponseErrorException("Received response retry=true", kafkaRecord);
            }

        } catch (IOException e) {
            log.warn("Error checking if Send Request was Successful.",e.getMessage());
            throw new ApiResponseErrorException(e.getLocalizedMessage());
        }
    }

    private RetryIndicator getResponseIndicator() {
        try {
            JsonObject response = new JsonParser().parse(getResponse()).getAsJsonObject();
            return response.get("retry").getAsBoolean()? RetryIndicator.RETRY : RetryIndicator.NO_RETRY ;
        }catch (Exception ex){
            log.warn("Json response with 'retry' field with not found. Assuming retry=true.");
            return RetryIndicator.UNKNOWN;
        }
    }

    private String getResponse() {
        try {
            return readResponse(connection.getInputStream(), ResponseTypes.SUCCESS);
        } catch (IOException e) {
            try {
                return readResponse(connection.getErrorStream(), ResponseTypes.ERROR);
            } catch (IOException e1) {
                log.error("Error Reading response. \n Error:{}", e1.getLocalizedMessage());
                throw new ApiResponseErrorException(e1.getLocalizedMessage(), kafkaRecord);
            }

        }
    }

    private String readResponse(InputStream stream, ResponseTypes responseType) throws IOException {
        StringBuilder sb;
        try (BufferedReader br = new BufferedReader(new InputStreamReader(stream, STREAM_ENCODING))) {
            sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            String response = sb.toString();
            log.info("Api {} Response:{}", responseType, response);
            return response;
        }
    }
}
