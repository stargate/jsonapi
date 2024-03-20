package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Operation that list all available namespaces into the {@link CommandStatus#EXISTING_NAMESPACES}
 * command status.
 *
 * @param cqlSessionCache CQLSession cache for keyspace fetching
 */
public record FindNamespacesOperations(CQLSessionCache cqlSessionCache) implements Operation {

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    return Uni.createFrom()
        .item(
            () -> {
              // get all existing keyspaces
              List<String> keyspacesList =
                  cqlSessionCache
                      .getSession(dataApiRequestInfo)
                      .getMetadata()
                      .getKeyspaces()
                      .keySet()
                      .stream()
                      .map(CqlIdentifier::asInternal)
                      .toList();
              return new Result(keyspacesList);
            });
  }

  // simple result wrapper
  private record Result(List<String> keyspaces) implements Supplier<CommandResult> {

    @Override
    public CommandResult get() {
      Map<CommandStatus, Object> statuses = Map.of(CommandStatus.EXISTING_NAMESPACES, keyspaces);
      return new CommandResult(statuses);
    }
  }
}
