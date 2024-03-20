package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static io.stargate.sgv2.jsonapi.exception.ErrorCode.VECTORIZECONFIG_CHECK_FAIL;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Refactored as seperate class that represent a collection property.
 *
 * @param collectionName
 * @param vectorEnabled
 * @param vectorSize
 * @param similarityFunction
 * @param vectorizeServiceName
 * @param modelName
 */
public record CollectionSettings(
    String collectionName,
    boolean vectorEnabled,
    int vectorSize,
    SimilarityFunction similarityFunction,
    String vectorizeServiceName,
    String modelName,
    IndexingConfig indexingConfig) {

  private static final CollectionSettings EMPTY =
      new CollectionSettings("", false, 0, null, null, null, null);

  public static CollectionSettings empty() {
    return EMPTY;
  }

  public DocumentProjector indexingProjector() {
    // IndexingConfig null if no indexing definitions: default, index all:
    if (indexingConfig == null) {
      return DocumentProjector.identityProjector();
    }
    // otherwise get lazily initialized indexing projector from config
    return indexingConfig.indexingProjector();
  }

  public record IndexingConfig(
      Set<String> allowed, Set<String> denied, Supplier<DocumentProjector> indexedProject) {
    public IndexingConfig(Set<String> allowed, Set<String> denied) {
      this(
          allowed,
          denied,
          Suppliers.memoize(() -> DocumentProjector.createForIndexing(allowed, denied)));
    }

    public DocumentProjector indexingProjector() {
      return indexedProject.get();
    }

    public static IndexingConfig fromJson(JsonNode jsonNode) {
      Set<String> allowed = new HashSet<>();
      Set<String> denied = new HashSet<>();
      if (jsonNode.has("allow")) {
        jsonNode.get("allow").forEach(node -> allowed.add(node.asText()));
      }
      if (jsonNode.has("deny")) {
        jsonNode.get("deny").forEach(node -> denied.add(node.asText()));
      }
      return new IndexingConfig(allowed, denied);
    }

    // Need to override to prevent comparison of the supplier
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o instanceof IndexingConfig other) {
        return Objects.equals(this.allowed, other.allowed)
            && Objects.equals(this.denied, other.denied);
      }
      return false;
    }
  }

  /**
   * The similarity function used for the vector index. This is only applicable if the vector index
   * is enabled.
   */
  public enum SimilarityFunction {
    COSINE,
    EUCLIDEAN,
    DOT_PRODUCT,
    UNDEFINED;

    public static SimilarityFunction fromString(String similarityFunction) {
      if (similarityFunction == null) return UNDEFINED;
      return switch (similarityFunction.toLowerCase()) {
        case "cosine" -> COSINE;
        case "euclidean" -> EUCLIDEAN;
        case "dot_product" -> DOT_PRODUCT;
        default -> throw new JsonApiException(
            ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME,
            ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME.getMessage() + similarityFunction);
      };
    }
  }

  public static CollectionSettings getCollectionSettings(
      TableMetadata table, ObjectMapper objectMapper) {
    // [jsonapi#639]: get internal name to avoid quoting of case-sensitive names
    String collectionName = table.getName().asInternal();
    // get vector column
    final Optional<ColumnMetadata> vectorColumn =
        table.getColumn(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME);
    boolean vectorEnabled = vectorColumn.isPresent();
    // if vector column exist
    if (vectorEnabled) {
      final int vectorSize = ((VectorType) vectorColumn.get().getType()).getDimensions();
      // get vector index
      IndexMetadata vectorIndex = null;
      Map<CqlIdentifier, IndexMetadata> indexMap = table.getIndexes();
      for (CqlIdentifier key : indexMap.keySet()) {
        if (key.asInternal().endsWith(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME)) {
          vectorIndex = indexMap.get(key);
          break;
        }
      }
      // default function
      CollectionSettings.SimilarityFunction function = CollectionSettings.SimilarityFunction.COSINE;
      if (vectorIndex != null) {
        final String functionName =
            vectorIndex.getOptions().get(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME);
        if (functionName != null)
          function = CollectionSettings.SimilarityFunction.fromString(functionName);
      }
      final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));
      return createCollectionSettings(
          collectionName, vectorEnabled, vectorSize, function, comment, objectMapper);
    } else { // if not vector collection
      // handling comment so get the indexing config from comment
      final String comment = (String) table.getOptions().get(CqlIdentifier.fromInternal("comment"));
      return createCollectionSettings(
          collectionName,
          vectorEnabled,
          0,
          CollectionSettings.SimilarityFunction.UNDEFINED,
          comment,
          objectMapper);
    }
  }

  public static CollectionSettings getCollectionSettings(
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction similarityFunction,
      String comment,
      ObjectMapper objectMapper) {
    return createCollectionSettings(
        collectionName, vectorEnabled, vectorSize, similarityFunction, comment, objectMapper);
  }

  public static CollectionSettings createCollectionSettings(
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      SimilarityFunction function,
      String comment,
      ObjectMapper objectMapper) {

    if (comment == null || comment.isBlank()) {
      return new CollectionSettings(
          collectionName, vectorEnabled, vectorSize, function, null, null, null);
    } else {
      String vectorizeServiceName = null;
      String modelName = null;
      JsonNode commentConfig;
      try {
        commentConfig = objectMapper.readTree(comment);
      } catch (JsonProcessingException e) {
        // This should never happen, already check if vectorize is a valid JSON
        throw new RuntimeException("Invalid json string, please check 'options' configuration.", e);
      }
      JsonNode vectorizeConfig = commentConfig.path("vectorize");
      if (!vectorizeConfig.isMissingNode()) {
        vectorizeServiceName = vectorizeConfig.path("service").textValue();
        JsonNode optionsNode = vectorizeConfig.path("options");
        modelName = optionsNode.path("modelName").textValue();
        if (!(vectorizeServiceName != null
            && !vectorizeServiceName.isEmpty()
            && modelName != null
            && !modelName.isEmpty())) {
          // This should never happen, VectorizeConfig check null, unless it fails
          throw new JsonApiException(
              VECTORIZECONFIG_CHECK_FAIL,
              "%s, please check 'vectorize' configuration."
                  .formatted(VECTORIZECONFIG_CHECK_FAIL.getMessage()));
        }
      }
      IndexingConfig indexingConfig = null;
      JsonNode indexing = commentConfig.path("indexing");
      if (!indexing.isMissingNode()) {
        indexingConfig = IndexingConfig.fromJson(indexing);
      }
      return new CollectionSettings(
          collectionName,
          vectorEnabled,
          vectorSize,
          function,
          vectorizeServiceName,
          modelName,
          indexingConfig);
    }
  }
}
