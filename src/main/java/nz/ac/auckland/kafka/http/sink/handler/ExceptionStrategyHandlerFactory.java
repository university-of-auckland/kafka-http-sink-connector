package nz.ac.auckland.kafka.http.sink.handler;

import nz.ac.auckland.kafka.http.sink.HttpSinkConnectorConfig;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class ExceptionStrategyHandlerFactory {

    public enum ExceptionStrategy{
        PROGRESS_BACK_OFF_DROP_MESSAGE,
        PROGRESS_BACK_OFF_STOP_TASK,
        DROP_MESSAGE,
        STOP_TASK
    }

    public static ExceptionHandler getInstance(HttpSinkConnectorConfig config, SinkTaskContext context){
        switch(config.exceptionStrategy){
            case PROGRESS_BACK_OFF_DROP_MESSAGE: return new ProgressiveBackoffDropStrategyHandler(config,context);
            case PROGRESS_BACK_OFF_STOP_TASK: return new ProgressiveBackoffStopTaskHandler(config,context);
            case DROP_MESSAGE: return new DropMessageStrategyHandler(context);
            case STOP_TASK: return new StopTaskStrategyHandler();
            default: throw new StrategyNotFoundException();
        }
    }


    private static class StrategyNotFoundException extends RuntimeException{
    }
}
