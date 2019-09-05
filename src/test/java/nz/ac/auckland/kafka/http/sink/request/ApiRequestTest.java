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
import java.nio.charset.Charset;
import java.util.List;

import static nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig.HEADER_SEPERATOR_DEFAULT;
import static org.mockito.Mockito.*;

class ApiRequestTest {


    @Mock
    HttpURLConnection connection;
    @Mock
    KafkaRecord kafkaRecord;
    @Mock
    OutputStream outputStreamMock;

    @BeforeEach
    void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void Test_correct_header_plus_correlationId_topic_added(){

        String headers = "{\"Content-type\":\"application/json\",\"apikey\":\"API_KEY\"}";
        when(kafkaRecord.getTopic()).thenReturn("nz-ac-auckland-person");
        when(kafkaRecord.getOffset()).thenReturn(99L);
        when(kafkaRecord.getPartition()).thenReturn(0L);
        ArgumentCaptor<String> connectionPropsCaptor = ArgumentCaptor.forClass(String.class);

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);
        apiRequest.setHeaders(headers, "TRACE_ID", HEADER_SEPERATOR_DEFAULT);

        verify(connection, times(7))
                .setRequestProperty(connectionPropsCaptor.capture(),connectionPropsCaptor.capture());
        List<String> connProps = connectionPropsCaptor.getAllValues();
        Assertions.assertEquals("Content-type",connProps.get(0));
        Assertions.assertEquals("application/json",connProps.get(1));
        Assertions.assertEquals("apikey",connProps.get(2));
        Assertions.assertEquals("API_KEY",connProps.get(3));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_TRACE_ID_KEY,connProps.get(4));
        Assertions.assertEquals("TRACE_ID",connProps.get(5));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_INFO_KEY,connProps.get(10));
        Assertions.assertEquals("topic=nz-ac-auckland-person|partition=0|offset=99",connProps.get(11));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_KAFKA_TOPIC_KEY,connProps.get(12));
        Assertions.assertEquals("nz-ac-auckland-person",connProps.get(13));
    }

    @Test
    void Test_correct_header_when_noJson_custom_headers_passed(){

        String headers = "Content-type:application/json|apikey:API_KEY";
        when(kafkaRecord.getTopic()).thenReturn("nz-ac-auckland-person");
        when(kafkaRecord.getOffset()).thenReturn(99L);
        ArgumentCaptor<String> connectionPropsCaptor = ArgumentCaptor.forClass(String.class);

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);
        apiRequest.setHeaders(headers, "TRACE_ID", HEADER_SEPERATOR_DEFAULT);

        verify(connection, times(7))
                .setRequestProperty(connectionPropsCaptor.capture(),connectionPropsCaptor.capture());
        List<String> connProps = connectionPropsCaptor.getAllValues();
        Assertions.assertEquals("Content-type",connProps.get(0));
        Assertions.assertEquals("application/json",connProps.get(1));
        Assertions.assertEquals("apikey",connProps.get(2));
        Assertions.assertEquals("API_KEY",connProps.get(3));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_TRACE_ID_KEY,connProps.get(4));
        Assertions.assertEquals("TRACE_ID",connProps.get(5));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_INFO_KEY,connProps.get(10));
        Assertions.assertEquals("topic=nz-ac-auckland-person|partition=0|offset=99",connProps.get(11));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_KAFKA_TOPIC_KEY,connProps.get(12));
        Assertions.assertEquals("nz-ac-auckland-person",connProps.get(13));
    }

    @Test
    void Test_correct_header_when_no_custom_headers_passed(){

        String headers = "";
        when(kafkaRecord.getTopic()).thenReturn("nz-ac-auckland-person");
        when(kafkaRecord.getOffset()).thenReturn(99L);
        ArgumentCaptor<String> connectionPropsCaptor = ArgumentCaptor.forClass(String.class);

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);
        apiRequest.setHeaders(headers, "TRACE_ID", HEADER_SEPERATOR_DEFAULT);

        verify(connection, times(5))
                .setRequestProperty(connectionPropsCaptor.capture(),connectionPropsCaptor.capture());
        List<String> connProps = connectionPropsCaptor.getAllValues();
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_TRACE_ID_KEY,connProps.get(0));
        Assertions.assertEquals("TRACE_ID",connProps.get(1));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_INFO_KEY,connProps.get(6));
        Assertions.assertEquals("topic=nz-ac-auckland-person|partition=0|offset=99",connProps.get(7));
        Assertions.assertEquals(ApiRequest.REQUEST_HEADER_KAFKA_TOPIC_KEY,connProps.get(8));
        Assertions.assertEquals("nz-ac-auckland-person",connProps.get(9));
    }


    @Test
    void Test_sendPayload_throws_expectation_if_invalid_Json_response() throws IOException {


        String response = "{ \"message\":\"Unable to process. An unhandled exception has been thrown.\"," +
                "   \"retry\":false";
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);

        Assertions.assertThrows(ApiResponseErrorException.class, () ->
                apiRequest.sendPayload("test"));

    }

    @Test
    void Test_sendPayload_throws_expectation_if_text_response() throws IOException {


        String response = "error";
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);

        Assertions.assertThrows(ApiResponseErrorException.class, () ->
                apiRequest.sendPayload("test"));

    }

    @Test
    void Test_sendPayload_throws_expectation_if_non200_retry_true() throws IOException {


        String response = "{ \"message\":\"Unable to process. An unhandled exception has been thrown.\"," +
                "   \"retry\":true}";
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getInputStream()).thenCallRealMethod();
        when(connection.getErrorStream()).thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);

        Assertions.assertThrows(ApiResponseErrorException.class, () ->
                apiRequest.sendPayload("test"));

    }

    @Test
    void Test_sendPayload_throws_expectation_if_200_retry_true() throws IOException {


        String response = "{ \"message\":\"Unable to process. An unhandled exception has been thrown.\"," +
                "   \"retry\":true}";
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);

        Assertions.assertThrows(ApiResponseErrorException.class, () ->
                apiRequest.sendPayload("test"));

    }

    @Test
    void Test_sendPayload_does_not_throws_expectation_if_non200_retry_false() throws IOException {


        String response = "{ \"message\":\"User Not Found\"," +
                "   \"retry\":false}";
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);

        Assertions.assertDoesNotThrow (() -> apiRequest.sendPayload("test"));

    }

    @Test
    void Test_sendPayload_does_not_throws_expectation_if_200_retry_false() throws IOException {


        String response = "{ \"message\":\"OK\"," +
                "   \"retry\":false}";
        when(connection.getOutputStream()).thenReturn(outputStreamMock);
        when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(response.getBytes(Charset.forName("UTF-8"))));

        ApiRequest apiRequest = new ApiRequest(connection,kafkaRecord);

        Assertions.assertDoesNotThrow (() -> apiRequest.sendPayload("test"));

    }

}
