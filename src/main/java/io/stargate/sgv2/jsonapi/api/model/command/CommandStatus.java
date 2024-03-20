package io.stargate.sgv2.jsonapi.api.model.command;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Enum with it's json property name which is returned in api response inside status */
public enum CommandStatus {
  /** The element has the count of document */
  @JsonProperty("count")
  COUNTED_DOCUMENT,
  /** The element has the count of deleted documents */
  @JsonProperty("deletedCount")
  DELETED_COUNT,
  /** Status for reporting existing namespaces. */
  @JsonProperty("namespaces")
  EXISTING_NAMESPACES,
  /** Status for reporting existing collections. */
  @JsonProperty("collections")
  EXISTING_COLLECTIONS,
  /** The element has the list of inserted ids */
  @JsonProperty("insertedIds")
  INSERTED_IDS,
  /** The element has the count of document read for the update operation */
  @JsonProperty("matchedCount")
  MATCHED_COUNT,

  /** The element has the count of document modified for the update operation */
  @JsonProperty("modifiedCount")
  MODIFIED_COUNT,
  /**
   * The element with boolean 'true' represents if more document to be processed for updateMany and
   * deleteMany commands
   */
  @JsonProperty("moreData")
  MORE_DATA,
  /** The element has value 1 if collection is created */
  @JsonProperty("ok")
  OK,

  /** Next page state value that can be used in client side for pagination */
  @JsonProperty("nextPageState")
  PAGE_STATE,
  /**
   * The element has the document id of newly inserted document part of update, when upserted option
   * is 'true' and no document available in DB for matching condition
   */
  @JsonProperty("upsertedId")
  UPSERTED_ID,
  /**
   * The element has the session id of offline writer, which is used to write the data offline to
   * the SSTable files for example
   */
  @JsonProperty("offlineWriterSessionId")
  OFFLINE_WRITER_SESSION_ID,

  /**
   * The element has the status of offline writer session, which is used to write the data offline
   * to
   */
  @JsonProperty("offlineWriterSessionStatus")
  OFFLINE_WRITER_SESSION_STATUS
}
