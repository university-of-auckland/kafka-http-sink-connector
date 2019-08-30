package nz.ac.auckland.kafka.http.sink.validator;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class JsonValidatorTest {


    @Test
    void Test_ValidJsonString_Throws_No_Exception(){
        JsonValidator jsonValidator = new JsonValidator();
        String jsonString = "{\"Content-Type\":\"application/json\",\"apikey\":\"API_KEY\"}";

        Assertions.assertDoesNotThrow(() ->
                jsonValidator.ensureValid("callback.request.headers", jsonString));

    }

    // TODO: Enable test when only Json headers are accepted
    @Disabled
    @Test
    void Test_InValidValidJsonString_Throws_Exception(){
        JsonValidator jsonValidator = new JsonValidator();
        String jsonString = "{\"Content-Type\":\"application/json\",\"apikey\":\"API_KEY\"";

        Assertions.assertThrows(ConfigException.class, () ->
                jsonValidator.ensureValid("callback.request.headers", jsonString));

    }

    @Test
    void Test_EmptyJsonString_Throws_No_Exception(){
        JsonValidator jsonValidator = new JsonValidator();
        String jsonString = "";

        Assertions.assertDoesNotThrow(() ->
                jsonValidator.ensureValid("callback.request.headers", jsonString));

    }

    @Test
    void Test_NullJsonString_Throws_No_Exception(){
        JsonValidator jsonValidator = new JsonValidator();
        String jsonString = null;

        Assertions.assertDoesNotThrow(() ->
                jsonValidator.ensureValid("callback.request.headers", jsonString));

    }
}
