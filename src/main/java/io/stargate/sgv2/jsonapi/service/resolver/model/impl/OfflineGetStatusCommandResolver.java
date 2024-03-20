package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineGetStatusCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.OfflineGetStatusOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link OfflineGetStatusCommand}. */
@ApplicationScoped
public class OfflineGetStatusCommandResolver implements CommandResolver<OfflineGetStatusCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;

  @Inject
  public OfflineGetStatusCommandResolver(Shredder shredder, ObjectMapper objectMapper) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<OfflineGetStatusCommand> getCommandClass() {
    return OfflineGetStatusCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, OfflineGetStatusCommand command) {
    return new OfflineGetStatusOperation(ctx, command, shredder, objectMapper);
  }
}
