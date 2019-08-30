package nz.ac.auckland.kafka.http.sink.request;

public interface Request {

    Request setHeaders(String headers, String traceId);
    void sendPayload(String payload);
}
