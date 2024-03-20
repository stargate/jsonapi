package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.configuration.EmbeddingServiceConfigStore;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.Optional;
import java.util.function.Supplier;

public record CreateEmbeddingServiceOperation(
    Optional<String> tenant,
    EmbeddingServiceConfigStore embeddingServiceConfigStore,
    String serviceName,
    String providerName,
    String baseUrl,
    String apiKey)
    implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    EmbeddingServiceConfigStore.ServiceConfig serviceConfig =
        EmbeddingServiceConfigStore.ServiceConfig.provider(
            serviceName, providerName, apiKey, baseUrl);
    return Uni.createFrom()
        .item(serviceConfig)
        .onItem()
        .transform(
            a -> {
              embeddingServiceConfigStore.saveConfiguration(tenant(), serviceConfig);
              return new ServiceRegistrationResult();
            });
  }
}
