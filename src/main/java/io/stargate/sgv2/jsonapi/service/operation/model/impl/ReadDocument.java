package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import java.util.UUID;

/**
 * Represents a document read from the database
 *
 * @param id Document Id identifying the document
 * @param txnId Unique UUID resenting point in time of a document, used for LWT transactions
 * @param document JsonNode representation of the document
 * @param sortColumns List<JsonNode> Serialized sort column value
 * @param docJsonValue Grpc column value for doc_json.
 */
public record ReadDocument(
    DocumentId id,
    UUID txnId,
    JsonNode document,
    List<JsonNode> sortColumns,
    QueryOuterClass.Value docJsonValue) {

  public static ReadDocument from(DocumentId id, UUID txnId, JsonNode document) {
    return new ReadDocument(id, txnId, document, null, null);
  }

  public static ReadDocument from(
      DocumentId id, QueryOuterClass.Value docJsonValue, List<JsonNode> sortColumns) {
    return new ReadDocument(id, null, null, sortColumns, docJsonValue);
  }
}
