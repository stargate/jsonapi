package io.stargate.sgv2.jsonapi.service.shredding;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shred an incoming JSON document into the data we need to store in the DB, and then de-shred.
 *
 * <p>Implementation is based on the ideas from the earlier prototype, and extended to do things
 * like make better decisions about when to use a hash and when to use the actual value. i.e. a hash
 * of "a" is a lot longer than "a".
 *
 * <p>Note that currently document id ({@code _id}) is auto-generated using UUID random method if
 * incoming JSON does not contain it (otherwise passed-in {@code _id} is used as-is).
 */
@ApplicationScoped
public class Shredder {
  private final ObjectMapper objectMapper;

  private final DocumentLimitsConfig documentLimits;

  private final JsonProcessingMetricsReporter jsonProcessingMetricsReporter;

  @Inject
  public Shredder(
      ObjectMapper objectMapper,
      DocumentLimitsConfig documentLimits,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    this.objectMapper = objectMapper;
    this.documentLimits = documentLimits;
    this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
  }

  /**
   * Shreds a single JSON node into a {@link WritableShreddedDocument} representation.
   *
   * @param document {@link JsonNode} to shred.
   * @return WritableShreddedDocument
   */
  public WritableShreddedDocument shred(JsonNode document) {
    return shred(document, null);
  }

  public WritableShreddedDocument shred(JsonNode doc, UUID txId) {
    return shred(doc, txId, DocumentProjector.identityProjector(), "testCommand");
  }

  public WritableShreddedDocument shred(
      JsonNode doc, UUID txId, DocumentProjector indexProjector, String commandName) {
    // Although we could otherwise allow non-Object documents, requirement
    // to have the _id (or at least place for it) means we cannot allow that.
    if (!doc.isObject()) {
      throw ErrorCode.SHRED_BAD_DOCUMENT_TYPE.toApiException(
          "document to shred must be a JSON Object, instead got %s", doc.getNodeType());
    }

    final ObjectNode docWithId = normalizeDocumentId((ObjectNode) doc);
    final DocumentId docId = DocumentId.fromJson(docWithId.get(DocumentConstants.Fields.DOC_ID));
    final String docJson;

    // Now that we have the traversable document, verify it does not violate
    // structural limits, before serializing.
    // (note: value validation has to wait until no-indexing projection is applied)
    new FullDocValidator(documentLimits).validate(docWithId);

    // Need to re-serialize document now that _id is normalized.
    // Also unifies escaping and gets rid of pretty-printing (if any) to save storage space.
    try {
      // Important! Must use configured ObjectMapper for serialization, NOT JsonNode.toString()
      // (to use configuration we specify wrt serialization)
      docJson = objectMapper.writeValueAsString(docWithId);
    } catch (IOException e) { // never happens but signature exposes it
      throw new RuntimeException(e);
    }

    // And then we can validate the document size
    validateDocumentSize(documentLimits, docJson);

    // Create json bytes written metrics
    if (jsonProcessingMetricsReporter != null) { // TODO-SL
      jsonProcessingMetricsReporter.reportJsonWriteBytesMetrics(commandName, docJson.length());
    }

    final WritableShreddedDocument.Builder b =
        WritableShreddedDocument.builder(docId, txId, docJson, docWithId);

    // Before value validation, indexing, may need to drop "non-indexed" properties. But if so,
    // need to ensure we do not modify original document, so let's create a copy (may need
    // to be returned as "after" Document)
    ObjectNode indexableDocument;

    if (indexProjector != null) {
      indexableDocument = docWithId.deepCopy();
      indexProjector.applyProjection(indexableDocument);
    } else {
      // optimized case: if nothing to drop ("no-index"), can just use original
      indexableDocument = docWithId;
    }

    // and now we can finally validate (String) value lengths
    new IndexableValueValidator(documentLimits).validate(indexableDocument);

    // And finally let's traverse the document to actually "shred" (build index properties)
    traverse(indexableDocument, b, JsonPath.rootBuilder());
    return b.build();
  }

  /**
   * Method called to ensure that Document has Document Id (generating id if necessary), and that it
   * is the very first property in the document (reordering as needed). Note that a new document is
   * created and returned; input document is never modified.
   *
   * @param doc Document to use as the base
   * @return Document that has _id as its first property
   */
  private ObjectNode normalizeDocumentId(ObjectNode doc) {
    // First: see if we have Object Id present or not
    JsonNode idNode = doc.get(DocumentConstants.Fields.DOC_ID);

    // If not, generate one
    if (idNode == null) {
      idNode = generateDocumentId();
    }
    // Either way we need to construct actual document with _id as the first property;
    // unfortunately there is no way to reorder properties in-place.
    final ObjectNode docWithIdAsFirstProperty = objectMapper.createObjectNode();
    docWithIdAsFirstProperty.set(DocumentConstants.Fields.DOC_ID, idNode);
    // Ok to add all properties, possibly including doc id since order won't change
    docWithIdAsFirstProperty.setAll(doc);
    return docWithIdAsFirstProperty;
  }

  private JsonNode generateDocumentId() {
    // Currently we generate UUID-as-String; alternatively could use and create
    // ObjectId-compatible values for better interoperability
    return objectMapper.getNodeFactory().textNode(UUID.randomUUID().toString());
  }

  /**
   * Main traversal method we need to produce callbacks to passed-in listener; used to separate
   * shredding logic from that of recursive-descent traversal.
   */
  private void traverse(JsonNode doc, ShredListener callback, JsonPath.Builder pathBuilder) {
    // NOTE: main level is handled a bit differently; no callbacks for Objects or Arrays,
    // only for the (rare) case of atomic values. Just traversal.

    if (doc.isObject()) {
      traverseObject((ObjectNode) doc, callback, pathBuilder);
    } else if (doc.isArray()) {
      traverseArray((ArrayNode) doc, callback, pathBuilder);
    } else {
      traverseValue(doc, callback, pathBuilder);
    }
  }

  private void traverseObject(
      ObjectNode obj, ShredListener callback, JsonPath.Builder pathBuilder) {

    Iterator<Map.Entry<String, JsonNode>> it = obj.fields();
    while (it.hasNext()) {
      Map.Entry<String, JsonNode> entry = it.next();
      pathBuilder.property(entry.getKey());
      traverseValue(entry.getValue(), callback, pathBuilder);
    }
  }

  private void traverseArray(ArrayNode arr, ShredListener callback, JsonPath.Builder pathBuilder) {
    int ix = 0;
    for (JsonNode value : arr) {
      pathBuilder.index(ix++);
      traverseValue(value, callback, pathBuilder);
    }
  }

  private void traverseValue(JsonNode value, ShredListener callback, JsonPath.Builder pathBuilder) {
    final JsonPath path = pathBuilder.build();
    final String pathAsString = path.toString();

    if (pathAsString.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
      traverseVector(path, value, callback);
    } else if (pathAsString.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
      // Do nothing, vectorize property will just sit in doc json
    } else {
      if (value.isObject()) {
        ObjectNode ob = (ObjectNode) value;
        if (callback.shredObject(path, ob)) {
          traverseObject(ob, callback, pathBuilder.nestedObjectBuilder());
        }
      } else if (value.isArray()) {
        ArrayNode arr = (ArrayNode) value;
        callback.shredArray(path, arr);
        traverseArray(arr, callback, pathBuilder.nestedArrayBuilder());
      } else if (value.isTextual()) {
        callback.shredText(path, value.textValue());
      } else if (value.isNumber()) {
        callback.shredNumber(path, value.decimalValue());
      } else if (value.isBoolean()) {
        callback.shredBoolean(path, value.booleanValue());
      } else if (value.isNull()) {
        callback.shredNull(path);
      } else {
        throw ErrorCode.SHRED_UNRECOGNIZED_NODE_TYPE.toApiException(value.getNodeType().toString());
      }
    }
  }

  private void traverseVector(JsonPath path, JsonNode value, ShredListener callback) {
    if (value.isNull()) {
      return;
    }
    if (!value.isArray()) {
      throw ErrorCode.SHRED_BAD_DOCUMENT_VECTOR_TYPE.toApiException(value.getNodeType().toString());
    }
    ArrayNode arr = (ArrayNode) value;
    if (arr.size() == 0) {
      throw new JsonApiException(ErrorCode.SHRED_BAD_VECTOR_SIZE);
    }
    callback.shredVector(path, arr);
  }

  private void validateDocumentSize(DocumentLimitsConfig limits, String docJson) {
    // First: is the resulting document size (as serialized) too big?
    if (docJson.length() > limits.maxSize()) {
      throw ErrorCode.SHRED_DOC_LIMIT_VIOLATION.toApiException(
          "document size (%d chars) exceeds maximum allowed (%d)",
          docJson.length(), limits.maxSize());
    }
  }

  /**
   * Validator applied to the full document, before removing non-indexable properties. Used to
   * ensure that the full document does not violate overall structural limits such as total length
   * or maximum nesting depth, or invalid property names. Most checks are done at a later point with
   * {@link IndexableValueValidator}.
   */
  static class FullDocValidator {
    final DocumentLimitsConfig limits;

    public FullDocValidator(DocumentLimitsConfig limits) {
      this.limits = limits;
    }

    public void validate(ObjectNode doc) {
      // Second: traverse to check for other constraints
      validateObjectValue(null, doc, 0, 0);
    }

    private void validateValue(
        String referringPropertyName, JsonNode value, int depth, int parentPathLength) {
      if (value.isObject()) {
        validateObjectValue(referringPropertyName, value, depth, parentPathLength);
      } else if (value.isArray()) {
        validateArrayValue(referringPropertyName, value, depth, parentPathLength);
      }
    }

    private void validateArrayValue(
        String referringPropertyName, JsonNode arrayValue, int depth, int parentPathLength) {
      ++depth;
      validateDocDepth(limits, depth);

      // Array value size limit only applied for indexable, none checked here
      for (JsonNode element : arrayValue) {
        validateValue(null, element, depth, parentPathLength);
      }
    }

    private void validateObjectValue(
        String referringPropertyName, JsonNode objectValue, int depth, int parentPathLength) {
      ++depth;
      validateDocDepth(limits, depth);

      final int propCount = objectValue.size();

      var it = objectValue.fields();
      while (it.hasNext()) {
        var entry = it.next();
        final String key = entry.getKey();
        validateObjectKey(key, entry.getValue(), depth, parentPathLength);
        // Path through property consists of segments separated by comma:
        final int propPathLength = parentPathLength + 1 + key.length();
        validateValue(key, entry.getValue(), depth, propPathLength);
      }
    }

    private void validateObjectKey(String key, JsonNode value, int depth, int parentPathLength) {
      if (key.length() == 0) {
        // NOTE: validity failure, not size limit
        throw ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION.toApiException("empty names not allowed");
      }
      if (!DocumentConstants.Fields.VALID_NAME_PATTERN.matcher(key).matches()) {
        // Special names are accepted in some cases: for now only one such case for
        // Date values -- if more needed, will refactor. But for now inline:
        if (JsonUtil.EJSON_VALUE_KEY_DATE.equals(key) && value.isValueNode()) {
          ; // Fine, looks like legit Date value
        } else if (key.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD) && depth == 1) {
          ; // Fine, looks like legit vector property
        } else if (key.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD) && depth == 1) {
          ; // Fine, looks like legit vectorize property
        } else {
          throw ErrorCode.SHRED_DOC_KEY_NAME_VIOLATION.toApiException(
              "property name ('%s') contains character(s) not allowed", key);
        }
      }
      int totalPathLength = parentPathLength + key.length();
      if (totalPathLength > limits.maxPropertyPathLength()) {
        throw ErrorCode.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "property path length (%d) exceeds maximum allowed (%d) (path ends with '%s')",
            totalPathLength, limits.maxPropertyPathLength(), key);
      }
    }

    private void validateDocDepth(DocumentLimitsConfig limits, int depth) {
      if (depth > limits.maxDepth()) {
        throw ErrorCode.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "document depth exceeds maximum allowed (%s)", limits.maxDepth());
      }
    }
  }

  /**
   * Secondary validator applied to the storable document after non-indexable properties (and
   * branches) have been pruned.
   */
  static class IndexableValueValidator {
    final DocumentLimitsConfig limits;

    final AtomicInteger totalProperties;

    public IndexableValueValidator(DocumentLimitsConfig limits) {
      this.limits = limits;
      totalProperties = new AtomicInteger(0);
    }

    public void validate(ObjectNode doc) {
      validateObjectValue(null, doc);
      if (totalProperties.get() > limits.maxDocumentProperties()) {
        throw ErrorCode.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "total number of indexed properties (%d) in document exceeds maximum allowed (%d)",
            totalProperties.get(), limits.maxDocumentProperties());
      }
    }

    private void validateValue(String referringPropertyName, JsonNode value) {
      if (value.isObject()) {
        validateObjectValue(referringPropertyName, value);
      } else if (value.isArray()) {
        validateArrayValue(referringPropertyName, value);
      } else if (value.isTextual()) {
        validateStringValue(referringPropertyName, value.textValue());
      }
    }

    private void validateArrayValue(String referringPropertyName, JsonNode arrayValue) {
      if (arrayValue.size() > limits.maxArrayLength()) {
        // One special case: vector embeddings allow larger size
        if (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(referringPropertyName)) {
          if (arrayValue.size() > limits.maxVectorEmbeddingLength()) {
            throw ErrorCode.SHRED_DOC_LIMIT_VIOLATION.toApiException(
                "number of elements Vector embedding (property '%s') has (%d) exceeds maximum allowed (%d)",
                referringPropertyName, arrayValue.size(), limits.maxVectorEmbeddingLength());
          }
        } else {
          throw ErrorCode.SHRED_DOC_LIMIT_VIOLATION.toApiException(
              "number of elements an indexable Array (property '%s') has (%d) exceeds maximum allowed (%d)",
              referringPropertyName, arrayValue.size(), limits.maxArrayLength());
        }
      }

      for (JsonNode element : arrayValue) {
        validateValue(referringPropertyName, element);
      }
    }

    private void validateObjectValue(String referringPropertyName, JsonNode objectValue) {
      final int propCount = objectValue.size();
      if (propCount > limits.maxObjectProperties()) {
        throw ErrorCode.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "number of properties an indexable Object (property '%s') has (%d) exceeds maximum allowed (%s)",
            referringPropertyName, objectValue.size(), limits.maxObjectProperties());
      }
      totalProperties.addAndGet(propCount);

      for (Map.Entry<String, JsonNode> entry : objectValue.properties()) {
        validateValue(entry.getKey(), entry.getValue());
      }
    }

    private void validateStringValue(String referringPropertyName, String value) {
      OptionalInt encodedLength =
          JsonUtil.lengthInBytesIfAbove(value, limits.maxStringLengthInBytes());
      if (encodedLength.isPresent()) {
        throw ErrorCode.SHRED_DOC_LIMIT_VIOLATION.toApiException(
            "indexed String value (property '%s') length (%d bytes) exceeds maximum allowed (%d bytes)",
            referringPropertyName, encodedLength.getAsInt(), limits.maxStringLengthInBytes());
      }
    }
  }
}
