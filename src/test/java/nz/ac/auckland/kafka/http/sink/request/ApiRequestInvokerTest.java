package nz.ac.auckland.kafka.http.sink.request;

import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import nz.ac.auckland.kafka.http.sink.model.KafkaRecord;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig.HEADER_SEPERATOR_DEFAULT;
import static org.mockito.Mockito.*;

class ApiRequestInvokerTest {


    @Mock
    private SinkTaskContext sinkTaskContext;
    @Mock
    private  ApiRequest apiRequest;
    @Mock
    private ApiRequestBuilder apiRequestBuilder;

    private HttpSinkConnectorConfig  config;
    private Collection<SinkRecord> records;

    @BeforeEach
    void initMocks() {
        MockitoAnnotations.initMocks(this);
        records = Collections.singleton(new SinkRecord("nz-ac-auckland-person", 0,
                null, null, null, "{\"subject\":\"testUser\"}",0, 4L, TimestampType.CREATE_TIME));

        Map<String,String> props= new HashMap<>();
        props.put(HttpSinkConnectorConfig.HTTP_API_URL,"http://mockbin.com");
        props.put(HttpSinkConnectorConfig.REQUEST_METHOD,"POST");
        props.put(HttpSinkConnectorConfig.HEADERS,"");
        props.put(HttpSinkConnectorConfig.HEADER_SEPERATOR, HEADER_SEPERATOR_DEFAULT);
        props.put(HttpSinkConnectorConfig.EXCEPTION_STRATEGY,"PROGRESS_BACK_OFF_DROP_MESSAGE");
        config = new HttpSinkConnectorConfig(props);
    }


    @Test
    void Test_ApiResponseErrorException_throws_RetriableException(){

        when(apiRequestBuilder.createRequest(any(), any(KafkaRecord.class))).thenReturn(apiRequest);
        when(apiRequest.setHeaders(anyString(), anyString(), anyString())).thenReturn(apiRequest);

        doThrow(ApiResponseErrorException.class).when(apiRequest).sendPayload(anyString());

        ApiRequestInvoker invoker = new ApiRequestInvoker(config,sinkTaskContext, apiRequestBuilder);
        Assertions.assertThrows(RetriableException.class, () ->
                invoker.invoke(records));


        verify(apiRequestBuilder, times(1))
                .createRequest(any(),any(KafkaRecord.class));
    }

    @Test
    void Test_ApiRequestErrorException_throws_ConnectException(){

        when(apiRequestBuilder.createRequest(any(), any(KafkaRecord.class))).thenReturn(apiRequest);
        when(apiRequest.setHeaders(anyString(), anyString(), anyString())).thenReturn(apiRequest);

        doThrow(ApiRequestErrorException.class).when(apiRequest).sendPayload(anyString());

        ApiRequestInvoker invoker = new ApiRequestInvoker(config,sinkTaskContext, apiRequestBuilder);
        Assertions.assertThrows(ConnectException.class, () ->
                invoker.invoke(records));


        verify(apiRequestBuilder, times(1))
                .createRequest(any(),any(KafkaRecord.class));

    }

    @Test
    void Test_ApiRequestErrorException_throws_ConnectException_No_Timestamp(){

        records = Collections.singleton(new SinkRecord("nz-ac-auckland-person", 0,
                null, null, null, "{\"subject\":\"testUser\"}",0));

        when(apiRequestBuilder.createRequest(any(), any(KafkaRecord.class))).thenReturn(apiRequest);
        when(apiRequest.setHeaders(anyString(), anyString(), anyString())).thenReturn(apiRequest);

        doThrow(ApiRequestErrorException.class).when(apiRequest).sendPayload(anyString());

        ApiRequestInvoker invoker = new ApiRequestInvoker(config,sinkTaskContext, apiRequestBuilder);
        Assertions.assertThrows(ConnectException.class, () ->
                invoker.invoke(records));


        verify(apiRequestBuilder, times(1))
                .createRequest(any(),any(KafkaRecord.class));

    }
}
