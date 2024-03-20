package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ReadAndUpdateOperationRetryTest extends OperationTestBase {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private CommandContext COMMAND_CONTEXT;

  @Inject Shredder shredder;
  @Inject ObjectMapper objectMapper;

  private final ColumnDefinitions KEY_TXID_JSON_COLUMNS =
      buildColumnDefs(
          OperationTestBase.TestColumn.keyColumn(),
          OperationTestBase.TestColumn.ofUuid("tx_id"),
          OperationTestBase.TestColumn.ofVarchar("doc_json"));

  @PostConstruct
  public void init() {
    COMMAND_CONTEXT =
        new CommandContext(
            KEYSPACE_NAME, COLLECTION_NAME, "testCommand", jsonProcessingMetricsReporter);
  }

  private MockRow resultRow(ColumnDefinitions columnDefs, int index, Object... values) {
    List<ByteBuffer> buffers = Stream.of(values).map(value -> byteBufferFromAny(value)).toList();
    return new MockRow(columnDefs, index, buffers);
  }

  private MockRow resultRow(int index, String key, UUID txId, String doc) {
    return new MockRow(
        KEY_TXID_JSON_COLUMNS,
        index,
        Arrays.asList(byteBufferForKey(key), byteBufferFrom(txId), byteBufferFrom(doc)));
  }

  private final ColumnDefinitions COLUMNS_APPLIED =
      buildColumnDefs(OperationTestBase.TestColumn.ofBoolean("[applied]"));

  private SimpleStatement nonVectorUpdateStatement(
      WritableShreddedDocument shredDocument, UUID tx_id) {
    String updateCql = UPDATE.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    return SimpleStatement.newInstance(
        updateCql,
        CQLBindValues.getSetValue(shredDocument.existKeys()),
        CQLBindValues.getIntegerMapValues(shredDocument.arraySize()),
        CQLBindValues.getStringSetValue(shredDocument.arrayContains()),
        CQLBindValues.getBooleanMapValues(shredDocument.queryBoolValues()),
        CQLBindValues.getDoubleMapValues(shredDocument.queryNumberValues()),
        CQLBindValues.getStringMapValues(shredDocument.queryTextValues()),
        CQLBindValues.getSetValue(shredDocument.queryNullValues()),
        CQLBindValues.getTimestampMapValues(shredDocument.queryTimestampValues()),
        shredDocument.docJson(),
        CQLBindValues.getDocumentIdValue(shredDocument.id()),
        tx_id);
  }

  // TODO: as part of https://github.com/stargate/jsonapi/issues/214
  //  - non-lwt failure partial, full
  //  - non-lwt failure on retry
  private static String UPDATE =
      "UPDATE \"%s\".\"%s\" "
          + "        SET"
          + "            tx_id = now(),"
          + "            exist_keys = ?,"
          + "            array_size = ?,"
          + "            array_contains = ?,"
          + "            query_bool_values = ?,"
          + "            query_dbl_values = ?,"
          + "            query_text_values = ?,"
          + "            query_null_values = ?,"
          + "            query_timestamp_values = ?,"
          + "            doc_json  = ?"
          + "        WHERE "
          + "            key = ?"
          + "        IF "
          + "            tx_id = ?";

  @Test
  public void findOneAndUpdateWithRetry() throws Exception {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);

    // read1
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
    String doc1 =
        """
        {
          "_id": "doc1",
          "username": "user1"
        }
        """;

    SimpleStatement stmt1 =
        SimpleStatement.newInstance(
            collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());
    List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", tx_id1, doc1));
    AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
    final AtomicInteger selectQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
        .then(
            invocation -> {
              selectQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results1);
            });

    //     read2
    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    SimpleStatement stmt2 =
        SimpleStatement.newInstance(
            collectionReadCql,
            boundKeyForStatement("doc1"),
            "username " + new DocValueHasher().getHash("user1").hash());
    List<Row> rows2 = Arrays.asList(resultRow(0, "doc1", tx_id2, doc1));
    AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
    final AtomicInteger reReadQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
        .then(
            invocation -> {
              reReadQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results2);
            });

    // update
    String doc1Updated =
        """
            {
              "_id": "doc1",
              "username": "user1",
              "name" : "test"
            }
            """;
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
    SimpleStatement stmt3 = nonVectorUpdateStatement(shredDocument, tx_id1);
    List<Row> rows3 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results3 = new MockAsyncResultSet(COLUMNS_APPLIED, rows3, null);
    final AtomicInteger failedUpdateQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt3)))
        .then(
            invocation -> {
              failedUpdateQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results3);
            });

    SimpleStatement stmt4 = nonVectorUpdateStatement(shredDocument, tx_id2);
    List<Row> rows4 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
    AsyncResultSet results4 = new MockAsyncResultSet(COLUMNS_APPLIED, rows4, null);
    final AtomicInteger updateQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt4)))
        .then(
            invocation -> {
              updateQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results4);
            });

    LogicalExpression implicitAnd = LogicalExpression.and();
    implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
    List<DBFilterBase> filters =
        List.of(
            new DBFilterBase.TextFilter(
                "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
    implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

    FindOperation findOperation =
        FindOperation.unsortedSingle(
            COMMAND_CONTEXT,
            implicitAnd,
            DocumentProjector.identityProjector(),
            ReadType.DOCUMENT,
            objectMapper);

    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            false,
            shredder,
            DocumentProjector.identityProjector(),
            1,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(dataApiRequestInfo, queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    assertThat(selectQueryAssert.get()).isEqualTo(1);
    assertThat(reReadQueryAssert.get()).isEqualTo(1);
    assertThat(failedUpdateQueryAssert.get()).isEqualTo(1);
    assertThat(updateQueryAssert.get()).isEqualTo(1);

    // then result
    CommandResult result = execute.get();
    assertThat(result.status())
        .hasSize(2)
        .containsEntry(CommandStatus.MATCHED_COUNT, 1)
        .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
    assertThat(result.errors()).isNull();
  }

  @Test
  public void findAndUpdateWithRetryFailure() throws Exception {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);

    // read1
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
    String doc1 =
        """
            {
              "_id": "doc1",
              "username": "user1"
            }
            """;

    SimpleStatement stmt1 =
        SimpleStatement.newInstance(
            collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

    List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", tx_id1, doc1));
    AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
    final AtomicInteger selectQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
        .then(
            invocation -> {
              selectQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results1);
            });

    // read2
    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    SimpleStatement stmt2 =
        SimpleStatement.newInstance(
            collectionReadCql,
            boundKeyForStatement("doc1"),
            "username " + new DocValueHasher().getHash("user1").hash());

    List<Row> rows2 = Arrays.asList(resultRow(0, "doc1", tx_id2, doc1));
    AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
    final AtomicInteger reReadQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
        .then(
            invocation -> {
              reReadQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results2);
            });

    // update
    String doc1Updated =
        """
            {
              "_id": "doc1",
              "username": "user1",
              "name" : "test"
            }
            """;
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
    SimpleStatement stmt3 = nonVectorUpdateStatement(shredDocument, tx_id1);
    List<Row> rows3 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results3 = new MockAsyncResultSet(COLUMNS_APPLIED, rows3, null);
    final AtomicInteger updateFailedQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt3)))
        .then(
            invocation -> {
              updateFailedQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results3);
            });

    SimpleStatement stmt4 = nonVectorUpdateStatement(shredDocument, tx_id2);
    List<Row> rows4 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results4 = new MockAsyncResultSet(COLUMNS_APPLIED, rows4, null);
    final AtomicInteger updateRetryFailedQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt4)))
        .then(
            invocation -> {
              updateRetryFailedQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results4);
            });

    LogicalExpression implicitAnd = LogicalExpression.and();
    implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
    List<DBFilterBase> filters =
        List.of(
            new DBFilterBase.TextFilter(
                "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
    implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

    FindOperation findOperation =
        FindOperation.unsortedSingle(
            COMMAND_CONTEXT,
            implicitAnd,
            DocumentProjector.identityProjector(),
            ReadType.DOCUMENT,
            objectMapper);

    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            false,
            shredder,
            DocumentProjector.identityProjector(),
            1,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(dataApiRequestInfo, queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    assertThat(selectQueryAssert.get()).isEqualTo(1);
    assertThat(reReadQueryAssert.get()).isEqualTo(3);
    assertThat(updateFailedQueryAssert.get()).isEqualTo(1);
    assertThat(updateRetryFailedQueryAssert.get()).isEqualTo(3);

    // then result
    CommandResult result = execute.get();
    assertThat(result.status())
        .hasSize(2)
        .containsEntry(CommandStatus.MATCHED_COUNT, 1)
        .containsEntry(CommandStatus.MODIFIED_COUNT, 0);
    assertThat(result.errors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.fields()).containsEntry("errorCode", "CONCURRENCY_FAILURE");
              assertThat(error.message())
                  .isEqualTo(
                      "Failed to update documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
            });
  }

  @Test
  public void findAndUpdateWithRetryFailureWithUpsert() throws Exception {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);

    // read1
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
    String doc1 =
        """
            {
              "_id": "doc1",
              "username": "user1"
            }
            """;

    SimpleStatement stmt1 =
        SimpleStatement.newInstance(
            collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

    List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", tx_id1, doc1));
    AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
    final AtomicInteger selectQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
        .then(
            invocation -> {
              selectQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results1);
            });

    // read2
    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    SimpleStatement stmt2 =
        SimpleStatement.newInstance(
            collectionReadCql,
            boundKeyForStatement("doc1"),
            "username " + new DocValueHasher().getHash("user1").hash());

    List<Row> rows2 = Arrays.asList(resultRow(0, "doc1", tx_id2, doc1));
    AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
    final AtomicInteger reReadQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
        .then(
            invocation -> {
              reReadQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results2);
            });

    // update
    String doc1Updated =
        """
            {
              "_id": "doc1",
              "username": "user1",
              "name" : "test"
            }
            """;
    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
    SimpleStatement stmt3 = nonVectorUpdateStatement(shredDocument, tx_id1);
    List<Row> rows3 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results3 = new MockAsyncResultSet(COLUMNS_APPLIED, rows3, null);
    final AtomicInteger updateFailedQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt3)))
        .then(
            invocation -> {
              updateFailedQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results3);
            });

    SimpleStatement stmt4 = nonVectorUpdateStatement(shredDocument, tx_id2);
    List<Row> rows4 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results4 = new MockAsyncResultSet(COLUMNS_APPLIED, rows4, null);
    final AtomicInteger updateRetryFailedQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt4)))
        .then(
            invocation -> {
              updateRetryFailedQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results4);
            });

    LogicalExpression implicitAnd = LogicalExpression.and();
    implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
    List<DBFilterBase> filters =
        List.of(
            new DBFilterBase.TextFilter(
                "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
    implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

    FindOperation findOperation =
        FindOperation.unsortedSingle(
            COMMAND_CONTEXT,
            implicitAnd,
            DocumentProjector.identityProjector(),
            ReadType.DOCUMENT,
            objectMapper);

    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            true,
            shredder,
            DocumentProjector.identityProjector(),
            1,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(dataApiRequestInfo, queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    assertThat(selectQueryAssert.get()).isEqualTo(1);
    assertThat(reReadQueryAssert.get()).isEqualTo(3);
    assertThat(updateFailedQueryAssert.get()).isEqualTo(1);
    assertThat(updateRetryFailedQueryAssert.get()).isEqualTo(3);

    // then result
    CommandResult result = execute.get();
    assertThat(result.status())
        .hasSize(2)
        .containsEntry(CommandStatus.MATCHED_COUNT, 1)
        .containsEntry(CommandStatus.MODIFIED_COUNT, 0);
    assertThat(result.errors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.fields()).containsEntry("errorCode", "CONCURRENCY_FAILURE");
              assertThat(error.message())
                  .isEqualTo(
                      "Failed to update documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
            });
  }

  @Test
  public void findAndUpdateWithRetryPartialFailure() throws Exception {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
    UUID tx_id3 = UUID.randomUUID();
    String doc1 =
        """
            {
              "_id": "doc1",
              "username": "user1",
              "status" : "active"
            }
            """;

    String doc2 =
        """
            {
              "_id": "doc2",
              "username": "user2",
              "status" : "active"
            }
            """;

    String doc1Updated =
        """
            {
              "_id": "doc1",
              "username": "user1",
              "status" : "active",
              "name" : "test"
            }
            """;

    String doc2Updated =
        """
            {
              "_id": "doc2",
              "username": "user2",
              "status" : "active",
              "name" : "test"
            }
            """;

    SimpleStatement stmt1 =
        SimpleStatement.newInstance(
            collectionReadCql, "status " + new DocValueHasher().getHash("active").hash());
    List<Row> rows1 =
        Arrays.asList(resultRow(0, "doc1", tx_id1, doc1), resultRow(0, "doc2", tx_id3, doc2));
    AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
    final AtomicInteger selectQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
        .then(
            invocation -> {
              selectQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results1);
            });

    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 3"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    SimpleStatement stmt2 =
        SimpleStatement.newInstance(
            collectionReadCql,
            boundKeyForStatement("doc1"),
            "status " + new DocValueHasher().getHash("active").hash());
    List<Row> rows2 = Arrays.asList(resultRow(0, "doc1", tx_id2, doc1));
    AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
    final AtomicInteger reReadFirstQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
        .then(
            invocation -> {
              reReadFirstQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results2);
            });

    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
    SimpleStatement stmt3 = nonVectorUpdateStatement(shredDocument, tx_id1);
    List<Row> rows3 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results3 = new MockAsyncResultSet(COLUMNS_APPLIED, rows3, null);
    final AtomicInteger failedUpdateFirstQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt3)))
        .then(
            invocation -> {
              failedUpdateFirstQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results3);
            });

    SimpleStatement stmt4 = nonVectorUpdateStatement(shredDocument, tx_id2);
    List<Row> rows4 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results4 = new MockAsyncResultSet(COLUMNS_APPLIED, rows4, null);
    final AtomicInteger failedUpdateRetryFirstQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt4)))
        .then(
            invocation -> {
              failedUpdateRetryFirstQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results4);
            });

    jsonNode = objectMapper.readTree(doc2Updated);
    shredDocument = shredder.shred(jsonNode);

    SimpleStatement stmt5 = nonVectorUpdateStatement(shredDocument, tx_id3);
    List<Row> rows5 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
    AsyncResultSet results5 = new MockAsyncResultSet(COLUMNS_APPLIED, rows5, null);
    final AtomicInteger updateSecondQueryAssert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt5)))
        .then(
            invocation -> {
              updateSecondQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results5);
            });

    LogicalExpression implicitAnd = LogicalExpression.and();
    implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
    List<DBFilterBase> filters =
        List.of(
            new DBFilterBase.TextFilter(
                "status", DBFilterBase.MapFilterBase.Operator.EQ, "active"));
    implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

    FindOperation findOperation =
        FindOperation.unsorted(
            COMMAND_CONTEXT,
            implicitAnd,
            DocumentProjector.identityProjector(),
            null,
            3,
            3,
            ReadType.DOCUMENT,
            objectMapper);

    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            false,
            shredder,
            DocumentProjector.identityProjector(),
            2,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(dataApiRequestInfo, queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();

    // assert query execution
    assertThat(selectQueryAssert.get()).isEqualTo(1);
    assertThat(reReadFirstQueryAssert.get()).isEqualTo(3);
    assertThat(failedUpdateFirstQueryAssert.get()).isEqualTo(1);
    assertThat(failedUpdateRetryFirstQueryAssert.get()).isEqualTo(3);
    assertThat(updateSecondQueryAssert.get()).isEqualTo(1);

    //     then result
    CommandResult result = execute.get();
    assertThat(result.status())
        .hasSize(2)
        .containsEntry(CommandStatus.MATCHED_COUNT, 2)
        .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
    assertThat(result.errors())
        .singleElement()
        .satisfies(
            error -> {
              assertThat(error.fields()).containsEntry("errorCode", "CONCURRENCY_FAILURE");
              assertThat(error.message())
                  .isEqualTo(
                      "Failed to update documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
            });
  }

  @Test
  public void findOneAndUpdateWithRetryMultipleFailure() throws Exception {
    QueryExecutor queryExecutor = mock(QueryExecutor.class);
    String collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);

    UUID tx_id1 = UUID.randomUUID();
    UUID tx_id2 = UUID.randomUUID();
    UUID tx_id3 = UUID.randomUUID();
    UUID tx_id4 = UUID.randomUUID();
    String doc1 =
        """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "status" : "active"
                  }
                """;

    String doc2 =
        """
                  {
                    "_id": "doc2",
                    "username": "user2",
                    "status" : "active"
                  }
                """;

    String doc1Updated =
        """
                  {
                    "_id": "doc1",
                    "username": "user1",
                    "status" : "active",
                    "name" : "test"
                  }
                """;

    String doc2Updated =
        """
                  {
                    "_id": "doc2",
                    "username": "user2",
                    "status" : "active",
                    "name" : "test"
                  }
                """;
    SimpleStatement stmt1 =
        SimpleStatement.newInstance(
            collectionReadCql, "status " + new DocValueHasher().getHash("active").hash());
    List<Row> rows1 =
        Arrays.asList(resultRow(0, "doc1", tx_id1, doc1), resultRow(0, "doc2", tx_id3, doc2));
    AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
    final AtomicInteger selectQueryAssert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
        .then(
            invocation -> {
              selectQueryAssert.incrementAndGet();
              return Uni.createFrom().item(results1);
            });
    collectionReadCql =
        "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 3"
            .formatted(KEYSPACE_NAME, COLLECTION_NAME);
    SimpleStatement stmt2 =
        SimpleStatement.newInstance(
            collectionReadCql,
            boundKeyForStatement("doc1"),
            "status " + new DocValueHasher().getHash("active").hash());
    List<Row> rows2 = Arrays.asList(resultRow(0, "doc1", tx_id2, doc1));
    AsyncResultSet results2 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows2, null);
    final AtomicInteger retrySelectQueryDoc1Assert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt2), any(), anyInt()))
        .then(
            invocation -> {
              retrySelectQueryDoc1Assert.incrementAndGet();
              return Uni.createFrom().item(results2);
            });

    SimpleStatement stmt3 =
        SimpleStatement.newInstance(
            collectionReadCql,
            boundKeyForStatement("doc2"),
            "status " + new DocValueHasher().getHash("active").hash());
    List<Row> rows3 = Arrays.asList(resultRow(0, "doc2", tx_id4, doc2));
    AsyncResultSet results3 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows3, null);
    final AtomicInteger retrySelectQueryDoc2Assert = new AtomicInteger();
    when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt3), any(), anyInt()))
        .then(
            invocation -> {
              retrySelectQueryDoc2Assert.incrementAndGet();
              return Uni.createFrom().item(results3);
            });

    JsonNode jsonNode = objectMapper.readTree(doc1Updated);
    WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
    SimpleStatement stmt4 = nonVectorUpdateStatement(shredDocument, tx_id1);
    List<Row> rows4 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results4 = new MockAsyncResultSet(COLUMNS_APPLIED, rows4, null);
    final AtomicInteger updateQueryDoc1Assert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt4)))
        .then(
            invocation -> {
              updateQueryDoc1Assert.incrementAndGet();
              return Uni.createFrom().item(results4);
            });

    SimpleStatement stmt5 = nonVectorUpdateStatement(shredDocument, tx_id2);
    List<Row> rows5 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results5 = new MockAsyncResultSet(COLUMNS_APPLIED, rows5, null);
    final AtomicInteger updateRetryQueryDoc1Assert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt5)))
        .then(
            invocation -> {
              updateRetryQueryDoc1Assert.incrementAndGet();
              return Uni.createFrom().item(results5);
            });

    jsonNode = objectMapper.readTree(doc2Updated);
    shredDocument = shredder.shred(jsonNode);

    SimpleStatement stmt6 = nonVectorUpdateStatement(shredDocument, tx_id3);
    List<Row> rows6 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results6 = new MockAsyncResultSet(COLUMNS_APPLIED, rows6, null);
    final AtomicInteger updateQueryDoc2Assert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt6)))
        .then(
            invocation -> {
              updateQueryDoc2Assert.incrementAndGet();
              return Uni.createFrom().item(results6);
            });

    SimpleStatement stmt7 = nonVectorUpdateStatement(shredDocument, tx_id4);
    List<Row> rows7 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.FALSE));
    AsyncResultSet results7 = new MockAsyncResultSet(COLUMNS_APPLIED, rows7, null);
    final AtomicInteger updateRetryQueryDoc2Assert = new AtomicInteger();
    when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt7)))
        .then(
            invocation -> {
              updateRetryQueryDoc2Assert.incrementAndGet();
              return Uni.createFrom().item(results7);
            });

    LogicalExpression implicitAnd = LogicalExpression.and();
    implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
    List<DBFilterBase> filters =
        List.of(
            new DBFilterBase.TextFilter(
                "status", DBFilterBase.MapFilterBase.Operator.EQ, "active"));
    implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

    FindOperation findOperation =
        FindOperation.unsorted(
            COMMAND_CONTEXT,
            implicitAnd,
            DocumentProjector.identityProjector(),
            null,
            3,
            3,
            ReadType.DOCUMENT,
            objectMapper);
    DocumentUpdater documentUpdater =
        DocumentUpdater.construct(
            DocumentUpdaterUtils.updateClause(
                UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
    ReadAndUpdateOperation operation =
        new ReadAndUpdateOperation(
            COMMAND_CONTEXT,
            findOperation,
            documentUpdater,
            true,
            false,
            false,
            shredder,
            DocumentProjector.identityProjector(),
            2,
            3);

    Supplier<CommandResult> execute =
        operation
            .execute(dataApiRequestInfo, queryExecutor)
            .subscribe()
            .withSubscriber(UniAssertSubscriber.create())
            .awaitItem()
            .getItem();
    final CommandResult commandResultSupplier = execute.get();

    assertThat(selectQueryAssert.get()).isEqualTo(1);
    assertThat(retrySelectQueryDoc1Assert.get()).isEqualTo(3);
    assertThat(updateQueryDoc1Assert.get()).isEqualTo(1);
    assertThat(updateRetryQueryDoc1Assert.get()).isEqualTo(3);

    assertThat(retrySelectQueryDoc2Assert.get()).isEqualTo(3);
    assertThat(updateQueryDoc2Assert.get()).isEqualTo(1);
    assertThat(updateRetryQueryDoc2Assert.get()).isEqualTo(3);

    assertThat(commandResultSupplier)
        .satisfies(
            commandResult -> {
              assertThat(commandResultSupplier.status()).isNotNull();
              assertThat(commandResultSupplier.status().get(CommandStatus.MATCHED_COUNT))
                  .isEqualTo(2);
              assertThat(commandResultSupplier.status().get(CommandStatus.MODIFIED_COUNT))
                  .isEqualTo(0);
              assertThat(commandResultSupplier.errors()).isNotNull();
              assertThat(commandResultSupplier.errors()).hasSize(1);
              assertThat(commandResultSupplier.errors().get(0).fields().get("errorCode"))
                  .isEqualTo("CONCURRENCY_FAILURE");
              assertThat(commandResultSupplier.errors().get(0).message())
                  .isEqualTo(
                      "Failed to update documents with _id ['doc1', 'doc2']: Unable to complete transaction due to concurrent transactions");
            });
  }
}
