package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Reports metrics related to JSON byte sizes and operation counts for various commands. Utilizes
 * Micrometer's {@link MeterRegistry} for metric registration and reporting, allowing integration
 * with various monitoring systems. Metrics include JSON bytes written/read and counts of JSON
 * write/read operations, tagged with command and tenant information.
 */
@ApplicationScoped
public class JsonBytesMetricsReporter {
  private static final String UNKNOWN_VALUE = "unknown";
  private final MeterRegistry meterRegistry;
  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final StargateRequestInfo stargateRequestInfo;
  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;

  @Inject
  public JsonBytesMetricsReporter(
      MeterRegistry meterRegistry,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      StargateRequestInfo stargateRequestInfo,
      MetricsConfig metricsConfig) {
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    this.stargateRequestInfo = stargateRequestInfo;
    tenantConfig = metricsConfig.tenantRequestCounter();
  }

  public void reportJsonWriteBytesMetrics(String commandName, long docJsonSize) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonBytesWritten())
            .tags(getCustomTags(commandName))
            .register(meterRegistry);
    ds.record(docJsonSize);
  }

  public void reportJsonReadBytesMetrics(String commandName, long docJsonSize) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonBytesRead())
            .tags(getCustomTags(commandName))
            .register(meterRegistry);
    ds.record(docJsonSize);
  }

  public void reportJsonWrittenCountMetrics(String commandName, int docCount) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonDocsWritten())
            .tags(getCustomTags(commandName))
            .register(meterRegistry);
    ds.record(docCount);
  }

  public void reportJsonReadCountMetrics(String commandName, int docCount) {
    DistributionSummary ds =
        DistributionSummary.builder(jsonApiMetricsConfig.jsonDocsRead())
            .tags(getCustomTags(commandName))
            .register(meterRegistry);
    ds.record(docCount);
  }

  private Tags getCustomTags(String commandName) {
    Tag tenantTag =
        Tag.of(tenantConfig.tenantTag(), stargateRequestInfo.getTenantId().orElse(UNKNOWN_VALUE));
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), commandName);
    return Tags.of(commandTag, tenantTag);
  }
}
