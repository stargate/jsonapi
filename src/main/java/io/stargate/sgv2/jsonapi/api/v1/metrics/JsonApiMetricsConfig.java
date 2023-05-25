package io.stargate.sgv2.jsonapi.api.v1.metrics;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

@ConfigMapping(prefix = "stargate.metrics")
public interface JsonApiMetricsConfig {
  @NotNull
  @Valid
  JsonApiMetricsConfig.CustomMetricsConfig customMetricsConfig();

  public interface CustomMetricsConfig {
    @NotBlank
    @WithDefault("error.class")
    String errorClass();

    @NotBlank
    @WithDefault("error.code")
    String errorCode();

    @NotBlank
    @WithDefault("command")
    String command();

    @NotBlank
    @WithDefault("command.processor.process")
    String metricsName();
  }
}