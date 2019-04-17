package nz.ac.auckland.kafka.http.sink.request;

public interface Request {

    Request setHeaders(String headers, String headerSeparator);
    void sendPayload(String payload);
}
