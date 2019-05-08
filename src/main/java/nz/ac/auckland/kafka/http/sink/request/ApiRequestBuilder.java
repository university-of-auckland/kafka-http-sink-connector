package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;

import java.net.HttpURLConnection;
import java.net.URL;

public class ApiRequestBuilder implements RequestBuilder {

    public  ApiRequest createRequest(HttpSinkConnectorConfig config, KafkaRecord kafkaRecord){

        try {
            URL url = new URL(config.httpApiUrl);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestMethod( config.requestMethod.toString());
            connection.setConnectTimeout( config.connectTimeout);
            connection.setReadTimeout(config.readTimeout);
            return new ApiRequest(connection,kafkaRecord);
        }catch (Exception e) {
            throw new ApiRequestErrorException(e.getLocalizedMessage());
        }
    }
}
