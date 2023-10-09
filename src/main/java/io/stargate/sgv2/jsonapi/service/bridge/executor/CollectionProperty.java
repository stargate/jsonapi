package io.stargate.sgv2.jsonapi.service.bridge.executor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.Schema;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.Optional;

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
public record CollectionProperty(
    String collectionName,
    Boolean vectorEnabled,
    int vectorSize,
    SimilarityFunction similarityFunction,
    String vectorizeServiceName,
    String modelName) {

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

  public static CollectionProperty getVectorProperties(
      Schema.CqlTable table, ObjectMapper objectMapper) {
    String collectionName = table.getName();
    final Optional<QueryOuterClass.ColumnSpec> first =
        table.getColumnsList().stream()
            .filter(
                c -> c.getName().equals(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME))
            .findFirst();
    boolean vectorEnabled = first.isPresent();
    if (vectorEnabled) {
      final int vectorSize = first.get().getType().getVector().getSize();
      final Optional<Schema.CqlIndex> vectorIndex =
          table.getIndexesList().stream()
              .filter(
                  i ->
                      i.getColumnName()
                          .equals(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME))
              .findFirst();
      CollectionProperty.SimilarityFunction function = CollectionProperty.SimilarityFunction.COSINE;
      if (vectorIndex.isPresent()) {

        if (vectorIndex
            .get()
            .getOptionsMap()
            .containsKey(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME)) {
          function =
              CollectionProperty.SimilarityFunction.fromString(
                  vectorIndex
                      .get()
                      .getOptions()
                      .get(DocumentConstants.Fields.VECTOR_INDEX_FUNCTION_NAME));
        }
      }
      final String comment = table.getOptionsOrDefault("comment", null);
      if (comment != null && !comment.isBlank()) {
        try {
          JsonNode vectorizeConfig = objectMapper.readTree(comment);
          String vectorizeServiceName =
              vectorizeConfig != null && vectorizeConfig.has("service")
                  ? vectorizeConfig.get("service").textValue()
                  : null;
          String modelName = null;
          final JsonNode optionsNode =
              vectorizeConfig != null && vectorizeConfig.has("options")
                  ? vectorizeConfig.get("options")
                  : null;
          if (optionsNode != null && optionsNode.has("modelName")) {
            modelName = optionsNode.get("modelName").textValue();
          }
          return new CollectionProperty(
              collectionName, vectorEnabled, vectorSize, function, vectorizeServiceName, modelName);
        } catch (JsonProcessingException e) {
          // This should never happen
          throw new RuntimeException(e);
        }
      } else {
        return new CollectionProperty(
            collectionName, vectorEnabled, vectorSize, function, null, null);
      }
    } else {
      return new CollectionProperty(
          collectionName,
          vectorEnabled,
          0,
          CollectionProperty.SimilarityFunction.UNDEFINED,
          null,
          null);
    }
  }
}
