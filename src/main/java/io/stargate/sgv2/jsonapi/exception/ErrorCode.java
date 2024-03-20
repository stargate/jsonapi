package io.stargate.sgv2.jsonapi.exception;

/** ErrorCode is our internal enum that provides codes and a default message for that error code. */
public enum ErrorCode {
  /** Command error codes. */
  COUNT_READ_FAILED("Unable to count documents"),
  COMMAND_NOT_IMPLEMENTED("The provided command is not implemented."),
  INVALID_CREATE_COLLECTION_OPTIONS("The provided options are invalid"),
  NO_COMMAND_MATCHED("Unable to find the provided command"),
  COMMAND_ACCEPTS_NO_OPTIONS("Command accepts no options"),

  /**
   * Error code used for {@code ConstraintViolationException} failures mapped to {@code
   * JsonApiException}
   */
  COMMAND_FIELD_INVALID("Request invalid"),

  CONCURRENCY_FAILURE("Unable to complete transaction due to concurrent transactions"),
  COLLECTION_NOT_EXIST("Collection does not exist, collection name: "),
  DATASET_TOO_BIG("Response data set too big to be sorted, add more filters"),

  DOCUMENT_ALREADY_EXISTS("Document already exists with the given _id"),

  DOCUMENT_UNPARSEABLE("Unable to parse the document"),

  DOCUMENT_REPLACE_DIFFERENT_DOCID(
      "The replace document and document resolved using filter have different _id"),

  FILTER_UNRESOLVABLE("Unable to resolve the filter"),

  FILTER_MULTIPLE_ID_FILTER(
      "Cannot have more than one _id equals filter clause: use $in operator instead"),

  FILTER_FIELDS_LIMIT_VIOLATION("Filter fields size limitation violated"),

  INVALID_REQUEST("Request not supported by the data store"),

  INVALID_INDEXING_DEFINITION("Invalid indexing definition"),

  UNINDEXED_FILTER_PATH("Unindexed filter path"),

  UNINDEXED_SORT_PATH("Unindexed sort path"),

  ID_NOT_INDEXED("_id is not indexed"),

  NAMESPACE_DOES_NOT_EXIST("The provided namespace does not exist."),

  SHRED_BAD_DOCUMENT_TYPE("Bad document type to shred"),

  SHRED_BAD_DOCID_TYPE("Bad type for '_id' property"),

  SHRED_BAD_DOCUMENT_VECTOR_TYPE("Bad $vector document type to shred "),

  SHRED_BAD_DOCUMENT_VECTORIZE_TYPE("Bad $vectorize document type to shred "),

  SHRED_BAD_DOCID_EMPTY_STRING("Bad value for '_id' property: empty String not allowed"),

  SHRED_INTERNAL_NO_PATH("Internal: path being built does not point to a property or element"),

  SHRED_NO_MD5("MD5 Hash algorithm not available"),

  SHRED_UNRECOGNIZED_NODE_TYPE("Unrecognized JSON node type in input document"),

  SHRED_DOC_LIMIT_VIOLATION("Document size limitation violated"),

  SHRED_DOC_KEY_NAME_VIOLATION("Document key name constraints violated"),

  SHRED_BAD_EJSON_VALUE("Bad EJSON value"),

  SHRED_BAD_VECTOR_SIZE("$vector value can't be empty"),

  SHRED_BAD_VECTOR_VALUE("$vector value needs to be array of numbers"),
  SHRED_BAD_VECTORIZE_VALUE("$vectorize search needs to be text value"),

  INVALID_FILTER_EXPRESSION("Invalid filter expression"),

  INVALID_COLLECTION_NAME("Invalid collection name"),

  INVALID_JSONAPI_COLLECTION_SCHEMA("Not a valid json api collection schema: "),

  TOO_MANY_COLLECTIONS("Too many collections"),

  TOO_MANY_INDEXES("Too many indexes"),

  UNSUPPORTED_FILTER_DATA_TYPE("Unsupported filter data type"),

  UNSUPPORTED_FILTER_OPERATION("Unsupported filter operator"),

  INVALID_SORT_CLAUSE_PATH("Invalid sort clause path"),

  INVALID_SORT_CLAUSE_VALUE(
      "Sort ordering value can only be `1` for ascending or `-1` for descending."),

  INVALID_USAGE_OF_VECTORIZE("`$vectorize` and `$vector` can't be used together."),

  UNSUPPORTED_OPERATION("Unsupported operation class"),

  UNSUPPORTED_PROJECTION_PARAM("Unsupported projection parameter"),

  UNSUPPORTED_UPDATE_DATA_TYPE("Unsupported update data type"),

  UNSUPPORTED_UPDATE_OPERATION("Unsupported update operation"),

  UNSUPPORTED_COMMAND_EMBEDDING_SERVICE(
      "Unsupported command `createEmbeddingService` since application is configured for property based embedding"),

  UNAVAILABLE_EMBEDDING_SERVICE("Unable to vectorize data, embedding service not available"),

  UNSUPPORTED_UPDATE_OPERATION_MODIFIER("Unsupported update operation modifier"),

  UNSUPPORTED_UPDATE_OPERATION_PARAM("Unsupported update operation parameter"),

  UNSUPPORTED_UPDATE_OPERATION_PATH("Invalid update operation path"),

  UNSUPPORTED_UPDATE_OPERATION_TARGET("Unsupported target JSON value for update operation"),

  UNSUPPORTED_UPDATE_FOR_DOC_ID("Cannot use operator with '_id' property"),

  UNSUPPORTED_UPDATE_FOR_VECTOR("Cannot use operator with '$vector' property"),
  UNSUPPORTED_UPDATE_FOR_VECTORIZE("Cannot use operator with '$vectorize' property"),

  VECTOR_SEARCH_NOT_AVAILABLE("Vector search functionality is not available in the backend"),

  VECTOR_SEARCH_USAGE_ERROR("Vector search can't be used with other sort clause"),

  VECTOR_SEARCH_NOT_SUPPORTED("Vector search is not enabled for the collection "),

  VECTOR_SEARCH_INVALID_FUNCTION_NAME("Invalid vector search function name: "),

  VECTOR_SEARCH_TOO_BIG_VALUE("Vector embedding property '$vector' length too big"),
  VECTORIZE_SERVICE_NOT_REGISTERED("Vectorize service name provided is not registered : "),

  VECTORIZE_SERVICE_TYPE_NOT_ENABLED("Vectorize service type not enabled : "),
  VECTORIZE_SERVICE_TYPE_UNSUPPORTED("Vectorize service type unsupported : "),

  VECTORIZE_SERVICE_TYPE_UNAVAILABLE("Vectorize service unavailable : "),
  VECTORIZE_USAGE_ERROR("Vectorize search can't be used with other sort clause"),

  VECTORIZECONFIG_CHECK_FAIL("Internal server error: VectorizeConfig check fail"),

  UNAUTHENTICATED_REQUEST("UNAUTHENTICATED: Invalid token"),
  INVALID_QUERY("Invalid query"),
  DRIVER_TIMEOUT("Driver timeout"),
  DRIVER_CLOSED_CONNECTION("Driver request connection is closed"),
  NO_NODE_AVAILABLE("No node was available to execute the query"),
  NO_INDEX_ERROR("Faulty collection (missing indexes). Recommend re-creating the collection"),
  COLLECTION_CREATION_ERROR(
      "Collection creation failure (unable to create table). Recommend re-creating the collection"),
  OFFLINE_WRITER_SESSION_NOT_FOUND("Offline writer session not found :"),
  UNABLE_TO_CREATE_OFFLINE_WRITER_SESSION("Unable to create offline writer session");

  private final String message;

  ErrorCode(String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  public JsonApiException toApiException(String format, Object... args) {
    return new JsonApiException(this, message + ": " + String.format(format, args));
  }

  public JsonApiException toApiException() {
    return new JsonApiException(this, message);
  }
}
