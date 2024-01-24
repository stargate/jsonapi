package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

public class JsonBytesMetricsReporter {
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  public JsonBytesMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public void createSizeMetrics(boolean writeFlag, String commandName, long docJsonSize) {
    String metricsName =
        writeFlag ? jsonApiMetricsConfig.jsonBytesWritten() : jsonApiMetricsConfig.jsonBytesRead();
    Counter counter =
        Counter.builder(metricsName)
            .description("The Json bytes metrics for '%s'".formatted(commandName))
            .tags(jsonApiMetricsConfig.command(), commandName)
            .register(meterRegistry);
    counter.increment(docJsonSize);
  }
}
