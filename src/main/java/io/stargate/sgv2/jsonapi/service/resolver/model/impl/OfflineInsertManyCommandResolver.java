package io.stargate.sgv2.jsonapi.service.resolver.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineInsertManyCommand;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;

/** Resolves the {@link OfflineInsertManyCommand}. */
@ApplicationScoped
public class OfflineInsertManyCommandResolver implements CommandResolver<OfflineInsertManyCommand> {

  private final Shredder shredder;
  private final ObjectMapper objectMapper;

  @Inject
  public OfflineInsertManyCommandResolver(Shredder shredder, ObjectMapper objectMapper) {
    this.shredder = shredder;
    this.objectMapper = objectMapper;
  }

  @Override
  public Class<OfflineInsertManyCommand> getCommandClass() {
    return OfflineInsertManyCommand.class;
  }

  @Override
  public Operation resolveCommand(CommandContext ctx, OfflineInsertManyCommand command) {
    // Vectorize documents
    ctx.tryVectorize(objectMapper.getNodeFactory(), command.documents());

    final DocumentProjector projection = ctx.indexingProjector();
    final List<WritableShreddedDocument> shreddedDocuments =
        command.documents().stream()
            .map(doc -> shredder.shred(doc, null, projection, command.getClass().getSimpleName()))
            .toList();

    boolean ordered =
        false; // TODO-SL this is a placeholder, we need to figure out how to handle this

    return new InsertOperation(ctx, shreddedDocuments, ordered);
  }
}
