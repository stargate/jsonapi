package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.DataVectorizer;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import java.util.List;

/**
 * Defines the context in which to execute the command.
 *
 * @param namespace The name of the namespace.
 * @param collection The name of the collection.
 * @param collectionSettings Settings for the collection, if Collection-specific command; if not,
 *     "empty" Settings {see CollectionSettings#empty()}.
 */
public record CommandContext(
    String namespace,
    String collection,
    CollectionSettings collectionSettings,
    EmbeddingService embeddingService,
    String commandName,
    JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {

  public CommandContext(String namespace, String collection) {
    this(namespace, collection, CollectionSettings.empty(), null, null, null);
  }

  public static CommandContext from(
      String namespace,
      String collection,
      CollectionSettings collectionSettings,
      EmbeddingService embeddingService,
      String commandName) {
    return new CommandContext(
        namespace, collection, collectionSettings, embeddingService, commandName, null);
  }

  public CommandContext(
      String namespace,
      String collection,
      String commandName,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    this(
        namespace,
        collection,
        CollectionSettings.empty(),
        null,
        commandName,
        jsonProcessingMetricsReporter);
  }

  private static final CommandContext EMPTY =
      new CommandContext(null, null, CollectionSettings.empty(), null, "testCommand", null);

  /**
   * @return Returns empty command context, having both {@link #namespace} and {@link #collection}
   *     as <code>null</code>.
   */
  public static CommandContext empty() {
    return EMPTY;
  }

  public CollectionSettings.SimilarityFunction similarityFunction() {
    return collectionSettings.similarityFunction();
  }

  public boolean isVectorEnabled() {
    return collectionSettings.vectorEnabled();
  }

  public DocumentProjector indexingProjector() {
    return collectionSettings.indexingProjector();
  }

  public void tryVectorize(JsonNodeFactory nodeFactory, List<JsonNode> documents) {
    new DataVectorizer(embeddingService(), nodeFactory).vectorize(documents);
  }

  public void tryVectorize(JsonNodeFactory nodeFactory, SortClause sortClause) {
    new DataVectorizer(embeddingService(), nodeFactory).vectorize(sortClause);
  }

  public void tryVectorize(JsonNodeFactory nodeFactory, UpdateClause updataClause) {
    new DataVectorizer(embeddingService(), nodeFactory).vectorizeUpdateClause(updataClause);
  }
}
