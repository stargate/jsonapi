package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public class JsonBytesMetricsReporter {
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  public JsonBytesMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public void createSizeMetrics(boolean writeFlag, String commandName, long docJsonSize) {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tags tags = Tags.of(commandTag);

    String metricsName =
        writeFlag ? jsonApiMetricsConfig.jsonBytesWritten() : jsonApiMetricsConfig.jsonBytesRead();
    Counter counter = Counter.builder(metricsName).tags(tags).register(meterRegistry);
    counter.increment(docJsonSize);
  }
}
