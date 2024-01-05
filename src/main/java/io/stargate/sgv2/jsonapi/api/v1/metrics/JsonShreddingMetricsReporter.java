package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.inject.Inject;

public class JsonShreddingMetricsReporter {

  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private Timer.Sample sample;

  @Inject
  public JsonShreddingMetricsReporter(
      MeterRegistry meterRegistry, JsonApiMetricsConfig jsonApiMetricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
  }

  public void startTimer() {
    sample = Timer.start(meterRegistry);
  }

  public void stopTimer(WritableShreddedDocument doc, String commandName) {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    Tag serializationTag = Tag.of(jsonApiMetricsConfig.jsonProcessTypeTag(), "Serialization");
    Tags tags = Tags.of(commandTag, serializationTag);
    sample.stop(meterRegistry.timer(jsonApiMetricsConfig.jsonProcessMetricsName(), tags));
  }
}