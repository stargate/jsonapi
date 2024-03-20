package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.ModifyOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Operation that inserts one or more documents.
 *
 * @param commandContext Context that defines namespace and database.
 * @param documents Documents to insert.
 * @param ordered If insert should be ordered.
 */
public record InsertOperation(
    CommandContext commandContext,
    List<WritableShreddedDocument> documents,
    boolean ordered,
    boolean conditionalInsert)
    implements ModifyOperation {

  public InsertOperation(
      CommandContext commandContext, List<WritableShreddedDocument> documents, boolean ordered) {
    this(commandContext, documents, ordered, true);
  }

  public InsertOperation(CommandContext commandContext, WritableShreddedDocument document) {
    this(commandContext, List.of(document), false, true);
  }

  private InsertOperation(CommandContext commandContext) {
    this(commandContext, List.of(), false, false);
  }

  public static InsertOperation forCQL(CommandContext commandContext) {
    return new InsertOperation(commandContext);
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    final boolean vectorEnabled = commandContext().isVectorEnabled();
    if (!vectorEnabled && documents.stream().anyMatch(doc -> doc.queryVectorValues() != null)) {
      throw new JsonApiException(
          ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED,
          ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED.getMessage() + commandContext().collection());
    }
    // create json doc write metrics
    if (commandContext.jsonProcessingMetricsReporter() != null) { // TODO-SL
      commandContext
          .jsonProcessingMetricsReporter()
          .reportJsonWrittenDocsMetrics(commandContext().commandName(), documents.size());
    }
    if (ordered) {
      return insertOrdered(dataApiRequestInfo, queryExecutor, vectorEnabled);
    } else {
      return insertUnordered(dataApiRequestInfo, queryExecutor, vectorEnabled);
    }
  }

  // implementation for the ordered insert
  private Uni<Supplier<CommandResult>> insertOrdered(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor, boolean vectorEnabled) {

    // build query once
    final String query = buildInsertQuery(vectorEnabled);

    return Multi.createFrom()
        .iterable(documents)

        // concatenate to respect ordered
        .onItem()
        .transformToUni(
            doc ->
                insertDocument(dataApiRequestInfo, queryExecutor, query, doc, vectorEnabled)

                    // wrap item and failure
                    // the collection can decide how to react on failure
                    .onItemOrFailure()
                    .transform((id, t) -> Tuple2.of(doc, t)))
        .concatenate(false)

        // if no failures reduce to the op page
        .collect()
        .in(
            InsertOperationPage::new,
            (agg, in) -> {
              Throwable failure = in.getItem2();
              agg.aggregate(in.getItem1().id(), failure);

              if (failure != null) {
                throw new FailFastInsertException(agg, failure);
              }
            })

        // in case upstream propagated FailFastInsertException
        // return collected result
        .onFailure(FailFastInsertException.class)
        .recoverWithItem(
            e -> {
              // safe to cast, asserted class in onFailure
              FailFastInsertException failFastInsertException = (FailFastInsertException) e;
              return failFastInsertException.result;
            })

        // use object identity to resolve to Supplier<CommandResult>
        .map(i -> i);
  }

  // implementation for the unordered insert
  private Uni<Supplier<CommandResult>> insertUnordered(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor, boolean vectorEnabled) {
    // build query once
    String query = buildInsertQuery(vectorEnabled);
    return Multi.createFrom()
        .iterable(documents)

        // merge to make it parallel
        .onItem()
        .transformToUniAndMerge(
            doc ->
                insertDocument(dataApiRequestInfo, queryExecutor, query, doc, vectorEnabled)

                    // handle errors fail silent mode
                    .onItemOrFailure()
                    .transform((id, t) -> Tuple2.of(doc, t)))

        // then reduce here
        .collect()
        .in(InsertOperationPage::new, (agg, in) -> agg.aggregate(in.getItem1().id(), in.getItem2()))

        // use object identity to resolve to Supplier<CommandResult>
        .map(i -> i);
  }

  // inserts a single document
  private static Uni<DocumentId> insertDocument(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      String query,
      WritableShreddedDocument doc,
      boolean vectorEnabled) {
    // bind and execute
    SimpleStatement boundStatement = bindInsertValues(query, doc, vectorEnabled);
    return queryExecutor
        .executeWrite(dataApiRequestInfo, boundStatement)

        // ensure document was written, if no applied continue with error
        .onItem()
        .transformToUni(
            result -> {
              if (result.wasApplied()) {
                return Uni.createFrom().item(doc.id());
              } else {
                final UUID txId = result.one().getUuid("tx_id");
                if (doc.nextTxID().equals(txId)) {
                  return Uni.createFrom().item(doc.id());
                }
                Exception failure = new JsonApiException(ErrorCode.DOCUMENT_ALREADY_EXISTS);
                return Uni.createFrom().failure(failure);
              }
            });
  }

  // utility for building the insert query
  public String buildInsertQuery(boolean vectorEnabled) {
    if (vectorEnabled) {
      String insertWithVector =
          "INSERT INTO \"%s\".\"%s\""
              + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values, query_vector_value)"
              + " VALUES"
              + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
              + (conditionalInsert ? " IF NOT EXISTS" : "");
      return String.format(
          insertWithVector, commandContext.namespace(), commandContext.collection());
    } else {
      String insert =
          "INSERT INTO \"%s\".\"%s\""
              + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values)"
              + " VALUES"
              + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
              + (conditionalInsert ? " IF NOT EXISTS" : "");
      return String.format(insert, commandContext.namespace(), commandContext.collection());
    }
  }

  // utility for query binding
  private static SimpleStatement bindInsertValues(
      String query, WritableShreddedDocument doc, boolean vectorEnabled) {
    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    if (vectorEnabled) {
      return SimpleStatement.newInstance(
          query,
          CQLBindValues.getDocumentIdValue(doc.id()),
          doc.nextTxID(),
          doc.docJson(),
          CQLBindValues.getSetValue(doc.existKeys()),
          CQLBindValues.getIntegerMapValues(doc.arraySize()),
          CQLBindValues.getStringSetValue(doc.arrayContains()),
          CQLBindValues.getBooleanMapValues(doc.queryBoolValues()),
          CQLBindValues.getDoubleMapValues(doc.queryNumberValues()),
          CQLBindValues.getStringMapValues(doc.queryTextValues()),
          CQLBindValues.getSetValue(doc.queryNullValues()),
          CQLBindValues.getTimestampMapValues(doc.queryTimestampValues()),
          CQLBindValues.getVectorValue(doc.queryVectorValues()));
    } else {
      return SimpleStatement.newInstance(
          query,
          CQLBindValues.getDocumentIdValue(doc.id()),
          doc.nextTxID(),
          doc.docJson(),
          CQLBindValues.getSetValue(doc.existKeys()),
          CQLBindValues.getIntegerMapValues(doc.arraySize()),
          CQLBindValues.getStringSetValue(doc.arrayContains()),
          CQLBindValues.getBooleanMapValues(doc.queryBoolValues()),
          CQLBindValues.getDoubleMapValues(doc.queryNumberValues()),
          CQLBindValues.getStringMapValues(doc.queryTextValues()),
          CQLBindValues.getSetValue(doc.queryNullValues()),
          CQLBindValues.getTimestampMapValues(doc.queryTimestampValues()));
    }
  }

  // simple exception to propagate fail fast
  private static class FailFastInsertException extends RuntimeException {

    private final InsertOperationPage result;

    public FailFastInsertException(InsertOperationPage result, Throwable cause) {
      super(cause);
      this.result = result;
    }
  }
}
