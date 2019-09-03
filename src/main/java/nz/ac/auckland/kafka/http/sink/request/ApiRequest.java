package nz.ac.auckland.kafka.http.sink.request;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;


public class ApiRequest implements Request{

    static final String REQUEST_HEADER_CORRELATION_ID_KEY = "X-B3-TraceId";
    static final String REQUEST_HEADER_KAFKA_TOPIC_KEY = "X-Kafka-Topic";
    static final String REQUEST_HEADER_INFO_KEY = "X-B3-Info" ;
    private final static String STREAM_ENCODING = "UTF-8";
    private HttpURLConnection connection;
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private KafkaRecord kafkaRecord;
    private static final List<Integer> CALLBACK_API_DOWN_HTTP_STATUS_CODE = Arrays.asList(502,503,504,401,403,405);

    private enum ReponseTypes {ERROR , SUCCESS}


    ApiRequest(HttpURLConnection connection, KafkaRecord kafkaRecord) {
        this.connection = connection;
        this.kafkaRecord = kafkaRecord;
    }

    @Override
    public ApiRequest setHeaders(String headerString, String traceId, String separator) {

        try{
            if(headerString != null && headerString.trim().length() > 0 ) {
                log.debug("Processing headers: {}", headerString);
                JsonObject headers = new JsonParser().parse(headerString).getAsJsonObject();

                log.debug("headers: {}", headers.toString());
                for (String headerKey : headers.keySet()) {
                    log.debug("Setting header property: {}", headerKey);
                    connection.setRequestProperty(headerKey, headers.get(headerKey).getAsString());
                }
            }
        }catch(JsonSyntaxException ex){
            setNonJsonHeaders(headerString, separator);
        }
        addCorrelationIdHeader(traceId);
        addInfoHeader();
        addTopicHeader();
        return this;
    }

    private void setNonJsonHeaders(String headers, String separator){
        log.debug("Processing Non json headers: {}, separator: {} ", headers, separator);
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
        }catch (IllegalStateException | JsonSyntaxException s){
            log.error("Unable to validate response. \n Error:{}", s.getLocalizedMessage());
            throw new ApiResponseErrorException(s.getLocalizedMessage(), kafkaRecord);
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
            throw new ApiResponseErrorException("Unable to validate response.", kafkaRecord);
        }
    }

    private String getResponse() {
        try {
            return readResponse(connection.getInputStream(), ReponseTypes.SUCCESS);
        } catch (IOException e) {
            try {
                return readResponse(connection.getErrorStream(), ReponseTypes.ERROR);
            } catch (IOException e1) {
                log.error("Error Reading response. \n Error:{}", e1.getLocalizedMessage());
                throw new ApiResponseErrorException(e1.getLocalizedMessage(), kafkaRecord);
            }

        }
    }

    private String readResponse(InputStream stream, ReponseTypes responseType) throws IOException {
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
