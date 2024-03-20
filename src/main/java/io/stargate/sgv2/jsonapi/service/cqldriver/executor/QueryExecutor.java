package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class QueryExecutor {
  private static final Logger logger = LoggerFactory.getLogger(QueryExecutor.class);
  private final OperationsConfig operationsConfig;

  /** CQLSession cache. */
  private final CQLSessionCache cqlSessionCache;

  @Inject
  public QueryExecutor(CQLSessionCache cqlSessionCache, OperationsConfig operationsConfig) {
    this.cqlSessionCache = cqlSessionCache;
    this.operationsConfig = operationsConfig;
  }

  /**
   * Execute read query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @param pagingState - In case of pagination, the paging state needs to be passed to fetch
   *     subsequent pages
   * @param pageSize - page size
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeRead(
      DataApiRequestInfo dataApiRequestInfo,
      SimpleStatement simpleStatement,
      Optional<String> pagingState,
      int pageSize) {
    simpleStatement =
        simpleStatement
            .setPageSize(pageSize)
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads());
    if (pagingState.isPresent()) {
      simpleStatement =
          simpleStatement.setPagingState(ByteBuffer.wrap(decodeBase64(pagingState.get())));
    }
    return Uni.createFrom()
        .completionStage(
            cqlSessionCache.getSession(dataApiRequestInfo).executeAsync(simpleStatement));
  }

  /**
   * Execute count query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public CompletionStage<AsyncResultSet> executeCount(
      DataApiRequestInfo dataApiRequestInfo, SimpleStatement simpleStatement) {
    simpleStatement =
        simpleStatement
            .setExecutionProfileName("count")
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().reads());
    return cqlSessionCache.getSession(dataApiRequestInfo).executeAsync(simpleStatement);
  }

  /**
   * Execute vector search query with bound statement.
   *
   * @param simpleStatement - Simple statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @param pagingState - In case of pagination, the paging state needs to be passed to fetch
   *     subsequent pages
   * @param pageSize - page size
   * @return
   */
  public Uni<AsyncResultSet> executeVectorSearch(
      DataApiRequestInfo dataApiRequestInfo,
      SimpleStatement simpleStatement,
      Optional<String> pagingState,
      int pageSize) {
    simpleStatement =
        simpleStatement
            .setPageSize(pageSize)
            .setConsistencyLevel(operationsConfig.queriesConfig().consistency().vectorSearch());
    if (pagingState.isPresent()) {
      simpleStatement =
          simpleStatement.setPagingState(ByteBuffer.wrap(decodeBase64(pagingState.get())));
    }
    return Uni.createFrom()
        .completionStage(
            cqlSessionCache.getSession(dataApiRequestInfo).executeAsync(simpleStatement));
  }

  /**
   * Execute write query with bound statement.
   *
   * @param statement - Bound statement with query and parameters. The table name used in the query
   *     must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeWrite(
      DataApiRequestInfo dataApiRequestInfo, SimpleStatement statement) {
    return Uni.createFrom()
        .completionStage(
            cqlSessionCache
                .getSession(dataApiRequestInfo)
                .executeAsync(
                    statement
                        .setIdempotent(true)
                        .setConsistencyLevel(
                            operationsConfig.queriesConfig().consistency().writes())
                        .setSerialConsistencyLevel(
                            operationsConfig.queriesConfig().serialConsistency())));
  }

  /**
   * Execute schema change query with bound statement for create ddl statements.
   *
   * @param boundStatement - Bound statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeCreateSchemaChange(
      DataApiRequestInfo dataApiRequestInfo, SimpleStatement boundStatement) {
    return executeSchemaChange(dataApiRequestInfo, boundStatement, "create");
  }

  /**
   * Execute schema change query with bound statement for drop ddl statements.
   *
   * @param boundStatement - Bound statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeDropSchemaChange(
      DataApiRequestInfo dataApiRequestInfo, SimpleStatement boundStatement) {
    return executeSchemaChange(dataApiRequestInfo, boundStatement, "drop");
  }

  /**
   * Execute schema change query with bound statement for truncate ddl statements.
   *
   * @param boundStatement - Bound statement with query and parameters. The table name used in the
   *     query must have keyspace prefixed.
   * @return AsyncResultSet
   */
  public Uni<AsyncResultSet> executeTruncateSchemaChange(
      DataApiRequestInfo dataApiRequestInfo, SimpleStatement boundStatement) {
    return executeSchemaChange(dataApiRequestInfo, boundStatement, "truncate");
  }

  private Uni<AsyncResultSet> executeSchemaChange(
      DataApiRequestInfo dataApiRequestInfo, SimpleStatement boundStatement, String profile) {
    return Uni.createFrom()
        .completionStage(
            cqlSessionCache
                .getSession(dataApiRequestInfo)
                .executeAsync(
                    boundStatement
                        .setExecutionProfileName(profile)
                        .setIdempotent(true)
                        .setSerialConsistencyLevel(
                            operationsConfig.queriesConfig().consistency().schemaChanges())))
        .onFailure(
            error ->
                error instanceof DriverTimeoutException || error instanceof InvalidQueryException)
        .recoverWithUni(
            throwable -> {
              logger.error(
                  "Timeout/Invalid query executing schema change query : {}",
                  boundStatement.getQuery());
              SimpleStatement duplicate = SimpleStatement.newInstance(boundStatement.getQuery());
              return Uni.createFrom()
                  .item(throwable)
                  .onItem()
                  .delayIt()
                  .by(Duration.ofMillis(operationsConfig.databaseConfig().ddlRetryDelayMillis()))
                  .onItem()
                  .transformToUni(
                      v ->
                          Uni.createFrom()
                              .completionStage(
                                  cqlSessionCache
                                      .getSession(dataApiRequestInfo)
                                      .executeAsync(
                                          duplicate
                                              .setExecutionProfileName(profile)
                                              .setIdempotent(true)
                                              .setSerialConsistencyLevel(
                                                  operationsConfig
                                                      .queriesConfig()
                                                      .consistency()
                                                      .schemaChanges()))));
            })
        .onFailure(
            error ->
                error instanceof DriverTimeoutException || error instanceof InvalidQueryException)
        .retry()
        .atMost(2);
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace
   * @param collectionName
   * @return
   */
  protected Uni<Optional<TableMetadata>> getSchema(
      DataApiRequestInfo dataApiRequestInfo, String namespace, String collectionName) {
    KeyspaceMetadata keyspaceMetadata;
    try {
      keyspaceMetadata =
          cqlSessionCache
              .getSession(dataApiRequestInfo)
              .getMetadata()
              .getKeyspaces()
              .get(CqlIdentifier.fromInternal(namespace));
    } catch (Exception e) {
      return Uni.createFrom().failure(e);
    }
    // if namespace does not exist, throw error
    if (keyspaceMetadata == null) {
      return Uni.createFrom()
          .failure(
              new JsonApiException(
                  ErrorCode.NAMESPACE_DOES_NOT_EXIST,
                  "The provided namespace does not exist: " + namespace));
    }
    // else get the table
    return Uni.createFrom().item(keyspaceMetadata.getTable("\"" + collectionName + "\""));
  }

  /**
   * Gets the schema for the provided namespace and collection name
   *
   * @param namespace - namespace
   * @param collectionName - collection name
   * @return TableMetadata
   */
  protected Uni<TableMetadata> getCollectionSchema(
      DataApiRequestInfo dataApiRequestInfo, String namespace, String collectionName) {
    Optional<KeyspaceMetadata> keyspaceMetadata;
    if ((keyspaceMetadata =
            cqlSessionCache.getSession(dataApiRequestInfo).getMetadata().getKeyspace(namespace))
        .isPresent()) {
      Optional<TableMetadata> tableMetadata = keyspaceMetadata.get().getTable(collectionName);
      if (tableMetadata.isPresent()) {
        return Uni.createFrom().item(tableMetadata.get());
      }
    }
    return Uni.createFrom().nullItem();
  }

  private static byte[] decodeBase64(String base64encoded) {
    return Base64.getDecoder().decode(base64encoded);
  }

  public CQLSessionCache getCqlSessionCache() {
    return this.cqlSessionCache;
  }
}
