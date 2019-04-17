package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.util.List;

import static org.mockito.Mockito.*;

public class ApiRequestTest {


    @Mock
    HttpURLConnection connection;
    @Mock
    KafkaRecord kafkaRecord;
    @Mock
    OutputStream outputStreamMock;

    @BeforeEach
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void Test_correct_header_plus_correlationId_topic_added() throws MalformedURLException {

        String headers = "Context-type:application/json|apikey:XYZ";
        when(kafkaRecord.getTopic()).thenReturn("nz-ac-auckland-person");
        when(kafkaRecord.getOffset()).thenReturn(99L);
        ArgumentCaptor<String> connectionPropsCaptor = ArgumentCaptor.forClass(String.class);

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);
        apiRequest.setHeaders(headers,"\\|");

        verify(connection, times(4))
                .setRequestProperty(connectionPropsCaptor.capture(),connectionPropsCaptor.capture());
        List<String> connProps = connectionPropsCaptor.getAllValues();
        Assertions.assertEquals("Context-type",connProps.get(0));
        Assertions.assertEquals("application/json",connProps.get(1));
        Assertions.assertEquals("apikey",connProps.get(2));
        Assertions.assertEquals("XYZ",connProps.get(3));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_CORRELATION_ID_KEY,connProps.get(4));
        Assertions.assertEquals("nz-ac-auckland-person-99",connProps.get(5));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_KAFKA_TOPIC_KEY,connProps.get(6));
        Assertions.assertEquals("nz-ac-auckland-person",connProps.get(7));
    }

    @Test
    void Test_correct_header_when_no_custom_headers_passed() throws MalformedURLException {

        String headers = "";
        when(kafkaRecord.getTopic()).thenReturn("nz-ac-auckland-person");
        when(kafkaRecord.getOffset()).thenReturn(99L);
        ArgumentCaptor<String> connectionPropsCaptor = ArgumentCaptor.forClass(String.class);

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);
        apiRequest.setHeaders(headers,"\\|");

        verify(connection, times(2))
                .setRequestProperty(connectionPropsCaptor.capture(),connectionPropsCaptor.capture());
        List<String> connProps = connectionPropsCaptor.getAllValues();
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_CORRELATION_ID_KEY,connProps.get(0));
        Assertions.assertEquals("nz-ac-auckland-person-99",connProps.get(1));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_KAFKA_TOPIC_KEY,connProps.get(2));
        Assertions.assertEquals("nz-ac-auckland-person",connProps.get(3));
    }

    @Test
    void Test_sendPayload_throws_expectation_if_retry_true() throws IOException {


        String response = "{ \"message\":\"Unable to process. An unhandled exception has been thrown.\"," +
                "   \"retry\":true}";
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);

        Assertions.assertThrows(ApiResponseErrorException.class, () ->
            apiRequest.sendPayload("test"));

    }

    @Test
    void Test_sendPayload_does_not_throws_expectation_if_retry_false() throws IOException {


        String response = "{ \"message\":\"OK\"," +
                "   \"retry\":false}";
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);

        Assertions.assertDoesNotThrow (() -> apiRequest.sendPayload("test"));

    }

}
