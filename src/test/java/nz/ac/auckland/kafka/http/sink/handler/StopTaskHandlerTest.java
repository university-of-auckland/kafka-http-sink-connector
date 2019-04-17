package nz.ac.auckland.kafka.http.sink.handler;

import nz.ac.auckland.kafka.http.sink.request.ApiResponseErrorException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StopTaskHandlerTest {

    @Mock
    private SinkTaskContext sinkTaskContext;

    @BeforeEach
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void Test_task_stopped_without_retries_no_commit(){

        StopTaskHandler handler = new StopTaskHandler();

        Assertions.assertThrows(ConnectException.class, () -> handler.handel(new ApiResponseErrorException("Error")));

        verify(sinkTaskContext, times(0)).requestCommit();
    }

}
