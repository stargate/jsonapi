package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DeleteOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.FindOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher.FilterableResolver;
import java.util.List;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

/**
 * Resolves the {@link DeleteOneCommand } DeleteOne command implements Filterable to identify the
 * record to be deleted, Based on the filter condition a record will deleted
 */
@ApplicationScoped
public class DeleteOneCommandResolver extends FilterableResolver<DeleteOneCommand>
    implements CommandResolver<DeleteOneCommand> {

  private final OperationsConfig operationsConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public DeleteOneCommandResolver(OperationsConfig operationsConfig, ObjectMapper objectMapper) {
    this.operationsConfig = operationsConfig;
    this.objectMapper = objectMapper;
  }

  @Override
  public Operation resolveCommand(CommandContext commandContext, DeleteOneCommand command) {
    FindOperation findOperation = getFindOperation(commandContext, command);
    return DeleteOperation.delete(commandContext, findOperation, 1, operationsConfig.lwt().retries());
  }

  @Override
  public Class<DeleteOneCommand> getCommandClass() {
    return DeleteOneCommand.class;
  }

  private FindOperation getFindOperation(CommandContext commandContext, DeleteOneCommand command) {
    List<DBFilterBase> filters = resolve(commandContext, command);
    return new FindOperation(
        commandContext,
        filters,
        DocumentProjector.identityProjector(),
        null,
        1,
        1,
        ReadType.KEY,
        objectMapper,
        null,
        0,
        0);
  }
}
