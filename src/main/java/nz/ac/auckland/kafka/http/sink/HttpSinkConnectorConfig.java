package nz.ac.auckland.kafka.http.sink;

import nz.ac.auckland.kafka.http.sink.handler.ResponseExceptionStrategyHandlerFactory;
import nz.ac.auckland.kafka.http.sink.validator.EnumValidator;
import nz.ac.auckland.kafka.http.sink.validator.JsonValidator;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class HttpSinkConnectorConfig extends AbstractConfig {


  public enum RequestMethod {
    POST,
    PUT,
    DELETE
  }

  public static final String HTTP_API_URL = "callback.request.url";
  private static final String HTTP_API_URL_DOC = "Callback request API URL.";
  private static final String HTTP_API_URL_DISPLAY = "Callback URL";

  public static final String REQUEST_METHOD = "callback.request.method";
  private static final String REQUEST_METHOD_DOC = "Callback Request Method.";
  private static final String REQUEST_METHOD_DISPLAY = "Callback request Method";

  public static final String HEADERS = "callback.request.headers";
  private static final String HEADERS_DOC = "Callback request headers."
            + "Default separator is |, use header.separator to modify this.";
  private static final String HEADERS_DISPLAY = "Callback request headers";

  //TODO: Removed header separator, allow only Json headers
  public static final String HEADER_SEPERATOR = "callback.header.separator";
  private static final String HEADER_SEPERATOR_DOC = "Separator character used in "
            + "headers property.";
  private static final String HEADER_SEPERATOR_DISPLAY = "Header separator";
  public static final String HEADER_SEPERATOR_DEFAULT = "\\|";

  private static final String CONNECT_TIMEOUT = "callback.timeout.connect.ms";
  private static final String CONNECT_TIMEOUT_DOC = "Connect timeout in ms when connecting to call back url.";
  private static final String CONNECT_TIMEOUT_DISPLAY = "Connect timeout (ms)";
  private static final String CONNECT_TIMEOUT_DEFAULT = "60000";

  private static final String READ_TIMEOUT = "callback.timeout.read.ms";
  private static final String READ_TIMEOUT_DOC = "Read timeout in ms when reading response from call back url.";
  private static final String READ_TIMEOUT_DISPLAY = "Read timeout (ms)";
  private static final String READ_TIMEOUT_DEFAULT = "60000";

  public static final String EXCEPTION_STRATEGY = "exception.strategy";
  private static final String EXCEPTION_STRATEGY_DEFAULT = ResponseExceptionStrategyHandlerFactory.ExceptionStrategy.PROGRESS_BACK_OFF_DROP_MESSAGE.toString();
  private static final String EXCEPTION_STRATEGY_DOC =
          "Exception strategy to handel retry response from API call.";
  private static final String EXCEPTION_STRATEGY_DISPLAY = "Exception strategy";

  public static final String RESPONSE_RETRY_BACKOFF_SEC = "retry.backoff.sec";
  private static final String RESPONSE_RETRY_BACKOFF_SEC_DEFAULT = "5,30,60,300,600";
  private static final String RESPONSE_RETRY_BACKOFF_SEC_DOC =
      "The time in seconds to wait following an error before a retry attempt is made for a errored response.";
  private static final String RESPONSE_RETRY_BACKOFF_SEC_DISPLAY = "Retry Backoff (secs)";

  public static final String REQUEST_RETRY_BACKOFF_SEC = "request.retry.backoff.sec";
  private static final String REQUEST_RETRY_BACKOFF_SEC_DEFAULT = "5,10,15";
  private static final String REQUEST_RETRY_BACKOFF_SEC_DOC =
          "The time in seconds to wait following an error before a retry attempt is made for a errored request.";
  private static final String REQUEST_RETRY_BACKOFF_SEC_DISPLAY = "Request Retry Backoff (secs)";

  public static final String DELAY_REQUEST_SEC = "request.delay.sec";
  private static final String DELAY_REQUEST_SEC_DEFAULT = "0";
  private static final String DELAY_REQUEST_SEC_DOC =
      "The time in seconds to wait before issuing a request based on the kafka event timestamp";
  private static final String DELAY_REQUEST_SEC_DISPLAY = "Request delay (secs)";

  private static final String API_REQUEST = "Request";
  private static final String RETRIES_GROUP = "Retries";

  private static final String RETRY_BACKOFF_SEC_SEPARATOR = ",";

  public final String httpApiUrl;
  public final RequestMethod requestMethod;
  public final int connectTimeout;
  public final int readTimeout;
  public final String headers;
  public final String headerSeparator;
  public final String[] responseRetryBackoffsec;
  public final String[] requestRetryBackoffsec;
  public final ResponseExceptionStrategyHandlerFactory.ExceptionStrategy exceptionStrategy;
  public final int requestDelay;

  public HttpSinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
    super(config, parsedConfig, false);
    httpApiUrl = getString(HTTP_API_URL);
    requestMethod = RequestMethod.valueOf(getString(REQUEST_METHOD).toUpperCase());
    connectTimeout = getInt(CONNECT_TIMEOUT);
    readTimeout = getInt(READ_TIMEOUT);
    headers = getString(HEADERS);
    headerSeparator = getString(HEADER_SEPERATOR);
    requestDelay = getInt(DELAY_REQUEST_SEC);
    responseRetryBackoffsec = getString(RESPONSE_RETRY_BACKOFF_SEC).split(RETRY_BACKOFF_SEC_SEPARATOR);
    requestRetryBackoffsec = getString(REQUEST_RETRY_BACKOFF_SEC).split(RETRY_BACKOFF_SEC_SEPARATOR);
    exceptionStrategy = ResponseExceptionStrategyHandlerFactory.ExceptionStrategy.valueOf(getString(EXCEPTION_STRATEGY).toUpperCase());
  }

  public HttpSinkConnectorConfig(Map<String, String> parsedConfig) {
    this(conf(), parsedConfig);
  }

  static ConfigDef conf() {
    return new ConfigDef()
            .define(
                HTTP_API_URL,
                ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH,
                HTTP_API_URL_DOC,
                API_REQUEST,
                1,
                ConfigDef.Width.LONG,
                HTTP_API_URL_DISPLAY
            ).define(
                REQUEST_METHOD,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                EnumValidator.in(RequestMethod.values()),
                ConfigDef.Importance.HIGH,
                REQUEST_METHOD_DOC,
                API_REQUEST,
                2,
                ConfigDef.Width.MEDIUM,
                REQUEST_METHOD_DISPLAY
            ).define(
                CONNECT_TIMEOUT,
                ConfigDef.Type.INT,
                CONNECT_TIMEOUT_DEFAULT,
                ConfigDef.Importance.HIGH,
                CONNECT_TIMEOUT_DOC,
                API_REQUEST,
                2,
                ConfigDef.Width.MEDIUM,
                CONNECT_TIMEOUT_DISPLAY
            ).define(
                READ_TIMEOUT,
                ConfigDef.Type.INT,
                READ_TIMEOUT_DEFAULT,
                ConfigDef.Importance.HIGH,
                READ_TIMEOUT_DOC,
                API_REQUEST,
                2,
                ConfigDef.Width.MEDIUM,
                READ_TIMEOUT_DISPLAY
            ).define(
                HEADERS,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new JsonValidator(),
                ConfigDef.Importance.HIGH,
                HEADERS_DOC,
                API_REQUEST,
                3,
                ConfigDef.Width.MEDIUM,
                HEADERS_DISPLAY
            ).define(
                HEADER_SEPERATOR,
                ConfigDef.Type.STRING,
                HEADER_SEPERATOR_DEFAULT,
                ConfigDef.Importance.HIGH,
                HEADER_SEPERATOR_DOC,
                API_REQUEST,
                4,
                ConfigDef.Width.SHORT,
                HEADER_SEPERATOR_DISPLAY
            ).define(
                RESPONSE_RETRY_BACKOFF_SEC,
                ConfigDef.Type.STRING,
                RESPONSE_RETRY_BACKOFF_SEC_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                RESPONSE_RETRY_BACKOFF_SEC_DOC,
                RETRIES_GROUP,
                1,
                ConfigDef.Width.SHORT,
                RESPONSE_RETRY_BACKOFF_SEC_DISPLAY
            ).define(
                REQUEST_RETRY_BACKOFF_SEC,
                ConfigDef.Type.STRING,
                REQUEST_RETRY_BACKOFF_SEC_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                REQUEST_RETRY_BACKOFF_SEC_DOC,
                RETRIES_GROUP,
                1,
                ConfigDef.Width.SHORT,
                REQUEST_RETRY_BACKOFF_SEC_DISPLAY
            )
            .define(
                EXCEPTION_STRATEGY,
                ConfigDef.Type.STRING,
                EXCEPTION_STRATEGY_DEFAULT,
                EnumValidator.in(ResponseExceptionStrategyHandlerFactory.ExceptionStrategy.values()),
                ConfigDef.Importance.MEDIUM,
                EXCEPTION_STRATEGY_DOC,
                RETRIES_GROUP,
                2,
                ConfigDef.Width.SHORT,
                EXCEPTION_STRATEGY_DISPLAY
            ).define(
                DELAY_REQUEST_SEC,
                ConfigDef.Type.INT,
                DELAY_REQUEST_SEC_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                DELAY_REQUEST_SEC_DOC,
                RETRIES_GROUP,
                2,
                ConfigDef.Width.SHORT,
                DELAY_REQUEST_SEC_DISPLAY
            );
  }
}
