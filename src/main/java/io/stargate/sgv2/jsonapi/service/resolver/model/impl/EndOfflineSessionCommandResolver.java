package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.EndOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.EndOfflineSessionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link EndOfflineSessionCommand}. */
@ApplicationScoped
public class EndOfflineSessionCommandResolver implements CommandResolver<EndOfflineSessionCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;

  @Inject
  public EndOfflineSessionCommandResolver(Shredder shredder, ObjectMapper objectMapper) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<EndOfflineSessionCommand> getCommandClass() {
    return EndOfflineSessionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, EndOfflineSessionCommand command) {
    return new EndOfflineSessionOperation(ctx, command, shredder, objectMapper);
  }
}
