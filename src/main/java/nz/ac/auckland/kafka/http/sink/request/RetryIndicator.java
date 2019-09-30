package nz.ac.auckland.kafka.http.sink.request;

public enum RetryIndicator {
    RETRY(true) , NO_RETRY(false), UNKNOWN(true);

    boolean shouldRetry;

    RetryIndicator(boolean retry){
        this.shouldRetry = retry;
    }
}
