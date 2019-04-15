package nz.ac.auckland.kafka.http.sink.validator;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EnumValidator implements ConfigDef.Validator {
    private final List<String> canonicalValues;
    private final Set<String> validValues;

    private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
        this.canonicalValues = canonicalValues;
        this.validValues = validValues;
    }

    public static <E> EnumValidator in(E[] enumerators) {
        final List<String> canonicalValues = new ArrayList<>(enumerators.length);
        final Set<String> validValues = new HashSet<>(enumerators.length * 2);
        for (E e : enumerators) {
            canonicalValues.add(e.toString());
            validValues.add(e.toString().toUpperCase());
        }
        return new EnumValidator(canonicalValues, validValues);
    }

    @Override
    public void ensureValid(String key, Object value) {
        if(value ==null || value.toString().trim().isEmpty() || !validValues.contains(value.toString().toUpperCase())){
            throw new ConfigException(key,value, "Valid values are " + toString());
        }
    }

    @Override
    public String toString() {
        return canonicalValues.toString();
    }
}