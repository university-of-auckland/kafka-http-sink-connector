package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.handler.StopTaskHandler;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;

public class ApiRequestBuilder implements RequestBuilder {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    public  ApiRequest createRequest(String uri, String httpMethod, KafkaRecord kafkaRecord){

        try {
            URL url = new URL(uri);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod(httpMethod);
            return new ApiRequest(connection,kafkaRecord);
        } catch (Exception e) {
            new StopTaskHandler().handel(new ApiResponseErrorException(e.getLocalizedMessage(),kafkaRecord));
        }
        return null;
    }
}
