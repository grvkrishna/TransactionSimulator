package org.grv.decorator;

import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.Executor;

public class TracingExecutor implements Executor {

    private final Executor delegate;


    public TracingExecutor(Executor delegate) {
        this.delegate = delegate;
    }


    @Override
    public void execute(Runnable command) {
        Map<String, String> captured = MDC.getCopyOfContextMap();
        delegate.execute(() -> {
            Map<String, String> previous = MDC.getCopyOfContextMap();
            if (captured != null) {
                MDC.setContextMap(captured);
            } else {
                MDC.clear();
            }
            try {
                command.run();
            } finally {
                if (previous != null) {
                    MDC.setContextMap(previous);
                } else {
                    MDC.clear();
                }
            }
        });
    }
}
