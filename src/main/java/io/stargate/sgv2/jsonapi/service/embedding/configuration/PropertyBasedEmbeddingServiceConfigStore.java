package io.stargate.sgv2.jsonapi.service.embedding.configuration;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

@LookupIfProperty(name = "stargate.jsonapi.embedding.config.store", stringValue = "property")
@ApplicationScoped
public class PropertyBasedEmbeddingServiceConfigStore implements EmbeddingServiceConfigStore {

  @Inject private PropertyBasedEmbeddingServiceConfig config;

  @Override
  public void saveConfiguration(Optional<String> tenant, ServiceConfig serviceConfig) {
    throw new UnsupportedOperationException("Not implemented");
  }

  public EmbeddingServiceConfigStore.ServiceConfig getConfiguration(
      Optional<String> tenant, String serviceName) {
    switch (serviceName) {
      case "openai":
        if (config.openai().enabled()) {
          return new ServiceConfig(
              serviceName, serviceName, config.openai().apiKey(), config.openai().url());
        }
        throw new JsonApiException(
            ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED,
            ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.getMessage() + serviceName);
      case "huggingface":
        if (config.hf().enabled()) {
          return new ServiceConfig(
              serviceName, serviceName, config.hf().apiKey(), config.hf().url());
        }
        throw new JsonApiException(
            ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED,
            ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.getMessage() + serviceName);
      case "vertexai":
        if (config.vertexai().enabled()) {
          return new ServiceConfig(
              serviceName, serviceName, config.vertexai().apiKey(), config.vertexai().url());
        }
        throw new JsonApiException(
            ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED,
            ErrorCode.VECTORIZE_SERVICE_TYPE_NOT_ENABLED.getMessage() + serviceName);
      default:
        throw new JsonApiException(
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNSUPPORTED,
            ErrorCode.VECTORIZE_SERVICE_TYPE_UNSUPPORTED.getMessage() + serviceName);
    }
  }
}
