package io.stargate.sgv2.jsonapi.service.processor;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes valid document {@link Command} to read, write, schema change, etc. This is a single
 * entry to run a Command without worrying how to.
 *
 * <p>Called from the API layer which deals with public JSON data formats, the command layer
 * translates from the JSON models to internal and back (shredding and de-shredding).
 *
 * <p>May provide a thread or resource boundary from calling API layer.
 */
@ApplicationScoped
public class CommandProcessor {

  private static Logger logger = LoggerFactory.getLogger(CommandProcessor.class);

  private final QueryExecutor queryExecutor;

  private final CommandResolverService commandResolverService;

  @Inject
  public CommandProcessor(
      QueryExecutor queryExecutor, CommandResolverService commandResolverService) {
    this.queryExecutor = queryExecutor;
    this.commandResolverService = commandResolverService;
  }

  /**
   * Processes a single command in a given command context.
   *
   * @param commandContext {@link CommandContext}
   * @param command {@link Command}
   * @return Uni emitting the result of the command execution.
   * @param <T> Type of the command.
   */
  public <T extends Command> Uni<CommandResult> processCommand(
      DataApiRequestInfo dataApiRequestInfo, CommandContext commandContext, T command) {
    // start by resolving the command, get resolver
    return commandResolverService
        .resolverForCommand(command)

        // resolver can be null, not handled in CommandResolverService for now
        .flatMap(
            resolver -> {
              // if we have resolver, resolve operation
              Operation operation = resolver.resolveCommand(commandContext, command);
              return Uni.createFrom().item(operation);
            })

        //  execute the operation
        .flatMap(operation -> operation.execute(dataApiRequestInfo, queryExecutor))

        // handle failures here
        .onFailure()
        .recoverWithItem(
            t -> {
              // DocsException is supplier of the CommandResult
              // so simply return
              if (t instanceof JsonApiException jsonApiException) {
                // Note: JsonApiException means that JSON API itself handled the situation
                // (created, or wrapped the exception) -- should not be logged (have already
                // been logged if necessary)
                return jsonApiException;
              }
              // But other exception types are unexpected, so log for now
              logger.warn(
                  "Command '{}' failed with exception", command.getClass().getSimpleName(), t);
              return new ThrowableCommandResultSupplier(t);
            })

        // if we have a non-null item
        // call supplier get to map to the command result
        .onItem()
        .ifNotNull()
        .transform(Supplier::get);
  }
}
