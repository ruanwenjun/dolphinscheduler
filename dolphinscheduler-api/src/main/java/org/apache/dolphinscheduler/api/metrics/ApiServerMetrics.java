package org.apache.dolphinscheduler.api.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ApiServerMetrics {

    private final Counter apiServerHeartbeatCounter =
        Counter.builder("ws.apiServer.heartbeat.count")
            .description("API Server heartbeat count")
            .register(Metrics.globalRegistry);

    public void incApiServerHeartbeatCounter() {
        apiServerHeartbeatCounter.increment();
    }
}
