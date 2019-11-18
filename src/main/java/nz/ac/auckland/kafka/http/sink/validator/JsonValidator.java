package nz.ac.auckland.kafka.http.sink.validator;

import com.google.gson.JsonParser;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonValidator implements ConfigDef.Validator  {

    private static final Logger log = LoggerFactory.getLogger(JsonValidator.class);


    @Override
    public void ensureValid(String key, Object json) {

        log.debug("validating json value for key: {}", key);
        if(json == null || json.toString().trim().length() == 0 ){
            return;
        }
        try {
            new JsonParser().parse(json.toString()).getAsJsonObject();
        }catch (Exception ex) {
            log.warn("Non Json header used");
            // TODO: DO not allow Non Json headers
            // throw new ConfigException(key,json, "Valid JSON string is required.");
        }
    }
}
