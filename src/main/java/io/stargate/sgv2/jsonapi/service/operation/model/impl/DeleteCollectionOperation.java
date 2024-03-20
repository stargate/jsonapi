package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the delete collection.
 *
 * @param context Command context, carries namespace of the collection.
 * @param name Collection name.
 */
public record DeleteCollectionOperation(CommandContext context, String name) implements Operation {
  private static final Logger logger = LoggerFactory.getLogger(DeleteCollectionOperation.class);

  private static final String DROP_TABLE_CQL = "DROP TABLE IF EXISTS \"%s\".\"%s\";";

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    logger.info("Executing DeleteCollectionOperation for {}", name);
    String cql = DROP_TABLE_CQL.formatted(context.namespace(), name);
    SimpleStatement query = SimpleStatement.newInstance(cql);
    // execute
    return queryExecutor
        .executeDropSchemaChange(dataApiRequestInfo, query)

        // if we have a result always respond positively
        .map(any -> new SchemaChangeResult(any.wasApplied()));
  }
}
