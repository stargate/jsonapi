package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.config.constants.TableCommentConstants;

/**
 * schema_version 0 is before we introduce schema_version into the C* table comment of data api
 * collection at this version, table comment only works for indexing options sample:
 * {"indexing":{"deny":["address"]}}
 */
public class CommandSettingsV0Deserializer implements CommandSettingsDeserializer {
  public CollectionSettings deserialize(
      JsonNode commentConfigNode,
      String collectionName,
      boolean vectorEnabled,
      int vectorSize,
      CollectionSettings.SimilarityFunction function) {
    CollectionSettings.VectorConfig vectorConfig =
        new CollectionSettings.VectorConfig(vectorEnabled, vectorSize, function, null);
    CollectionSettings.IndexingConfig indexingConfig = null;
    JsonNode indexing = commentConfigNode.path(TableCommentConstants.COLLECTION_INDEXING_KEY);
    if (!indexing.isMissingNode()) {
      indexingConfig = CollectionSettings.IndexingConfig.fromJson(indexing);
    }
    return new CollectionSettings(collectionName, vectorConfig, indexingConfig);
  }

  @Override
  public CollectionSettings deserialize(
      JsonNode jsonNode, String collectionName, ObjectMapper objectMapper) {
    return null;
  }
}