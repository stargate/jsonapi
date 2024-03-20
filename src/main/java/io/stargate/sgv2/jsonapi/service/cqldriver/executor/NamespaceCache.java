package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.schema.model.JsonapiTableMatcher;
import java.time.Duration;

/** Caches the vector enabled status for the namespace */
public class NamespaceCache {

  public final String namespace;

  public final QueryExecutor queryExecutor;

  private final ObjectMapper objectMapper;

  private static final long CACHE_TTL_SECONDS = 300;
  private static final long CACHE_MAX_SIZE = 1000;
  private final Cache<String, CollectionSettings> vectorCache =
      Caffeine.newBuilder()
          .expireAfterWrite(Duration.ofSeconds(CACHE_TTL_SECONDS))
          .maximumSize(CACHE_MAX_SIZE)
          .build();

  public NamespaceCache(String namespace, QueryExecutor queryExecutor, ObjectMapper objectMapper) {
    this.namespace = namespace;
    this.queryExecutor = queryExecutor;
    this.objectMapper = objectMapper;
  }

  protected Uni<CollectionSettings> getCollectionProperties(
      DataApiRequestInfo dataApiRequestInfo, String collectionName) {
    CollectionSettings collectionProperty = vectorCache.getIfPresent(collectionName);
    if (null != collectionProperty) {
      return Uni.createFrom().item(collectionProperty);
    } else {
      return getVectorProperties(dataApiRequestInfo, collectionName)
          .onItemOrFailure()
          .transformToUni(
              (result, error) -> {
                if (null != error) {
                  // not a valid collection schema
                  if (error instanceof JsonApiException
                      && ((JsonApiException) error).getErrorCode()
                          == ErrorCode.VECTORIZECONFIG_CHECK_FAIL) {
                    return Uni.createFrom()
                        .failure(
                            new JsonApiException(
                                ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA,
                                ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA
                                    .getMessage()
                                    .concat(collectionName)));
                  }
                  // collection does not exist
                  if (error instanceof RuntimeException rte
                      && rte.getMessage().startsWith(ErrorCode.COLLECTION_NOT_EXIST.getMessage())) {
                    return Uni.createFrom()
                        .failure(
                            new JsonApiException(
                                ErrorCode.COLLECTION_NOT_EXIST,
                                ErrorCode.COLLECTION_NOT_EXIST
                                    .getMessage()
                                    .concat(collectionName)));
                  }

                  // ignoring the error and return false. This will be handled while trying to
                  //  execute the query
                  if ((error instanceof StatusRuntimeException sre
                      && (sre.getStatus().getCode() == io.grpc.Status.Code.NOT_FOUND
                          || sre.getStatus().getCode() == io.grpc.Status.Code.INVALID_ARGUMENT))) {
                    return Uni.createFrom()
                        .item(
                            new CollectionSettings(
                                collectionName, false, 0, null, null, null, null));
                  }
                  return Uni.createFrom().failure(error);
                } else {
                  vectorCache.put(collectionName, result);
                  return Uni.createFrom().item(result);
                }
              });
    }
  }

  private Uni<CollectionSettings> getVectorProperties(
      DataApiRequestInfo dataApiRequestInfo, String collectionName) {
    return queryExecutor
        .getSchema(dataApiRequestInfo, namespace, collectionName)
        .onItem()
        .transform(
            table -> {
              if (table.isPresent()) {
                // check if its a valid json api table
                if (!new JsonapiTableMatcher().test(table.get())) {
                  throw new JsonApiException(
                      ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA,
                      ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA.getMessage() + collectionName);
                }
                return CollectionSettings.getCollectionSettings(table.get(), objectMapper);
              } else {
                throw new RuntimeException(
                    ErrorCode.COLLECTION_NOT_EXIST.getMessage() + collectionName);
              }
            });
  }
}
