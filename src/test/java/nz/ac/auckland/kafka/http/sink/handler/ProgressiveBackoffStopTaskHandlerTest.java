package nz.ac.auckland.kafka.http.sink.handler;

import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import nz.ac.auckland.kafka.http.sink.request.ApiResponseErrorException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ProgressiveBackoffStopTaskHandlerTest {

    @Mock
    private SinkTaskContext sinkTaskContext;

    HttpSinkConnectorConfig  config;

    @BeforeEach
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
        Map<String,String> props= new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL,"http://mockbin.com");
        props.put(HttpSinkConnectorConfig.REQUEST_METHOD,"POST");
        props.put(HttpSinkConnectorConfig.HEADERS,"");
        props.put(HttpSinkConnectorConfig.HEADER_SEPERATOR,"|");
        props.put(HttpSinkConnectorConfig.EXCEPTION_STRATEGY,"PROGRESS_BACK_OFF_DROP_MESSAGE");
        props.put(HttpSinkConnectorConfig.RETRY_BACKOFF_SEC,"5,10");
        config = new HttpSinkConnectorConfig(props);
    }

    @Test
    public void Test_task_stopped_after_set_retries_no_commit(){

        ProgressiveBackoffStopTaskHandler handler = new ProgressiveBackoffStopTaskHandler(config,sinkTaskContext);

        //First Try 5 sec
        Assertions.assertThrows(RetriableException.class, () -> invokeHandel(handler));
        //Second Try 10 sec
        Assertions.assertThrows(RetriableException.class, () -> invokeHandel(handler));
        //Stop task
        Assertions.assertThrows(ConnectException.class, () -> invokeHandel(handler));

        verify(sinkTaskContext, times(0)).requestCommit();
    }

    private void invokeHandel(ExceptionHandler handler){
        handler.handel(new ApiResponseErrorException("Error"));
    }

}
