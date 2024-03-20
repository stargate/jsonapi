package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.BeginOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.BeginOfflineSessionOperation;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/** Resolves the {@link BeginOfflineSessionCommand}. */
@ApplicationScoped
public class BeginOfflineSessionCommandResolver
    implements CommandResolver<BeginOfflineSessionCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;

  @Inject
  public BeginOfflineSessionCommandResolver(Shredder shredder, ObjectMapper objectMapper) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<BeginOfflineSessionCommand> getCommandClass() {
    return BeginOfflineSessionCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, BeginOfflineSessionCommand command) {
    return new BeginOfflineSessionOperation(
        ctx, command.getSessionId(), command.getFileWriterParams(), shredder, objectMapper);
  }
}
