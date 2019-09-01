package nz.ac.auckland.kafka.http.sink.util;

import javax.xml.bind.DatatypeConverter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class TraceIdGenerator {

    public static String generateTraceId(String hashString){

        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(hashString.getBytes());
            byte[] digest = md.digest();
            // As per APM standards keeping the trace limited to 16 chars.
            hashString = DatatypeConverter.printHexBinary(digest).toLowerCase().substring(0,16);
            return  hashString;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return  hashString;
    }
}
