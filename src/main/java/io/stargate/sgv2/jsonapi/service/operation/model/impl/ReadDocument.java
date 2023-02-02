package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.UUID;

/**
 * Represents a document read from the database
 *
 * @param id Document Id identifying the document
 * @param txnId Unique UUID resenting point in time of a document, used for LWT transactions
 * @param document JsonNode representation of the document
 */
public record ReadDocument(DocumentId id, UUID txnId, JsonNode document) {}
