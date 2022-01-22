package nz.ac.auckland.kafka.http.sink.handler;

import nz.ac.auckland.kafka.http.sink.request.ApiResponseErrorException;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DropMessageHandlerTest {

    @Mock
    private SinkTaskContext sinkTaskContext;

    @BeforeEach
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void Test_message_dropped_without_retries_and_committed(){

        DropMessageHandler handler = new DropMessageHandler(sinkTaskContext);

        Assertions.assertDoesNotThrow(() -> handler.handle(new ApiResponseErrorException("Error")));

        verify(sinkTaskContext, times(1)).requestCommit();
    }

}
