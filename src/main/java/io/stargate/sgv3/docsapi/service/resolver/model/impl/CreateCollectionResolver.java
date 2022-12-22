package io.stargate.sgv3.docsapi.service.resolver.model.impl;

import io.stargate.sgv3.docsapi.api.model.command.CommandContext;
import io.stargate.sgv3.docsapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv3.docsapi.service.operation.model.Operation;
import io.stargate.sgv3.docsapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv3.docsapi.service.resolver.model.CommandResolver;
import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class CreateCollectionResolver implements CommandResolver<CreateCollectionCommand> {
  @Override
  public Class<CreateCollectionCommand> getCommandClass() {
    return CreateCollectionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, CreateCollectionCommand command) {
    return new CreateCollectionOperation(ctx, command.name());
  }
}
