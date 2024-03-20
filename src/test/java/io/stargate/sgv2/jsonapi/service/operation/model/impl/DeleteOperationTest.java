package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.TupleValue;
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
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class DeleteOperationTest extends OperationTestBase {
  private CommandContext COMMAND_CONTEXT;
  @Inject ObjectMapper objectMapper;

  @PostConstruct
  public void init() {
    COMMAND_CONTEXT =
        new CommandContext(
            KEYSPACE_NAME, COLLECTION_NAME, "testCommand", jsonProcessingMetricsReporter);
  }

  @Nested
  class Execute {

    private final ColumnDefinitions DELETE_RESULT_COLUMNS =
        buildColumnDefs(TestColumn.ofBoolean("[applied]"));

    private final ColumnDefinitions SELECT_RESULT_COLUMNS =
        buildColumnDefs(TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"));

    private final ColumnDefinitions SELECT_WITH_JSON_RESULT_COLUMNS =
        buildColumnDefs(
            TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"), TestColumn.ofVarchar("doc_json"));

    @Test
    public void deleteWithId() {
      UUID tx_id = UUID.randomUUID();

      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, keyValue);

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue), byteBufferFrom(tx_id))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);
      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);
      assertThat(deleteCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
    }

    @Test
    public void deleteOneAndReturnById() {
      UUID tx_id = UUID.randomUUID();
      String docJson = "{\"_id\":\"doc1\",\"a\":1}";
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, keyValue);

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_WITH_JSON_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(keyValue), byteBufferFrom(tx_id), byteBufferFrom(docJson))));

      AsyncResultSet mockResults =
          new MockAsyncResultSet(SELECT_WITH_JSON_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      CommandContext commandContext =
          createCommandContextWithCommandName("jsonBytesReadDeleteCommand");
      FindOperation findOperation =
          FindOperation.unsortedSingle(
              commandContext,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      DeleteOperation operation =
          DeleteOperation.deleteOneAndReturn(
              commandContext, findOperation, 3, DocumentProjector.identityProjector());
      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);
      assertThat(deleteCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
      assertThat(result.data().getResponseDocuments()).hasSize(1);
      assertThat(result.data().getResponseDocuments().get(0).toString()).isEqualTo(docJson);

      // verify metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> jsonBytesReadMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_bytes_read")
                          && !line.startsWith("json_bytes_read_bucket")
                          && !line.contains("quantile")
                          && line.contains("jsonBytesReadDeleteCommand"))
              .toList();
      // should have three metrics in total
      assertThat(jsonBytesReadMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"jsonBytesReadDeleteCommand\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");
                    });
              });
      // verify count metric -- command called once, should be one
      List<String> jsonDocsReadCountMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_read_count")
                          && line.contains("jsonBytesReadDeleteCommand"))
              .toList();
      assertThat(jsonDocsReadCountMetrics).hasSize(1);
      jsonDocsReadCountMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(1.0);
          });
      // verify sum metric -- read one doc, should be one
      List<String> jsonDocsReadSumMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_read_sum")
                          && line.contains("jsonBytesReadDeleteCommand"))
              .toList();
      assertThat(jsonDocsReadSumMetrics).hasSize(1);
      jsonDocsReadSumMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(1.0);
          });
    }

    @Test
    public void deleteOneAndReturnWithSort() {
      ColumnDefinitions SELECT_SORT_RESULT_COLUMNS =
          buildColumnDefs(
              TestColumn.keyColumn(),
              TestColumn.ofUuid("tx_id"),
              TestColumn.ofVarchar("doc_json"),
              TestColumn.ofVarchar("query_text_values['username']"),
              TestColumn.ofDecimal("query_dbl_values['username']"),
              TestColumn.ofBoolean("query_bool_values['username']"),
              TestColumn.ofVarchar("query_null_values['username']"),
              TestColumn.ofTimestamp("query_timestamp_values['username']"));
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String docJson1 = "{\"_id\":\"doc1\",\"username\":1,\"status\":\"active\"}";
      String docJson2 = "{\"_id\":\"doc2\",\"username\":2,\"status\":\"active\"}";
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final TupleValue keyValue1 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      final TupleValue keyValue2 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "status " + new DocValueHasher().getHash("active").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(keyValue1),
                      byteBufferFrom(tx_id1),
                      byteBufferFrom(docJson1),
                      null,
                      byteBufferFrom(1),
                      null,
                      null,
                      null)),
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(keyValue2),
                      byteBufferFrom(tx_id2),
                      byteBufferFrom(docJson2),
                      null,
                      byteBufferFrom(2),
                      null,
                      null,
                      null)));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_SORT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue1, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.sortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              2,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              0,
              3);

      DeleteOperation operation =
          DeleteOperation.deleteOneAndReturn(
              COMMAND_CONTEXT, findOperation, 3, DocumentProjector.identityProjector());

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);
      assertThat(deleteCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
      assertThat(result.data().getResponseDocuments().get(0).toString()).isEqualTo(docJson1);
    }

    @Test
    public void deleteOneAndReturnWithSortDesc() {
      ColumnDefinitions SELECT_SORT_RESULT_COLUMNS =
          buildColumnDefs(
              TestColumn.keyColumn(),
              TestColumn.ofUuid("tx_id"),
              TestColumn.ofVarchar("doc_json"),
              TestColumn.ofVarchar("query_text_values['username']"),
              TestColumn.ofDecimal("query_dbl_values['username']"),
              TestColumn.ofBoolean("query_bool_values['username']"),
              TestColumn.ofVarchar("query_null_values['username']"),
              TestColumn.ofTimestamp("query_timestamp_values['username']"));
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String docJson1 = "{\"_id\":\"doc1\",\"username\":1,\"status\":\"active\"}";
      String docJson2 = "{\"_id\":\"doc2\",\"username\":2,\"status\":\"active\"}";
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final TupleValue keyValue1 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      final TupleValue keyValue2 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "status " + new DocValueHasher().getHash("active").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(keyValue1),
                      byteBufferFrom(tx_id1),
                      byteBufferFrom(docJson1),
                      null,
                      byteBufferFrom(1),
                      null,
                      null,
                      null)),
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(keyValue2),
                      byteBufferFrom(tx_id2),
                      byteBufferFrom(docJson2),
                      null,
                      byteBufferFrom(2),
                      null,
                      null,
                      null)));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_SORT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue2, tx_id2);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "status", DBFilterBase.MapFilterBase.Operator.EQ, "active"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.sortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              2,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", false)),
              0,
              3);

      DeleteOperation operation =
          DeleteOperation.deleteOneAndReturn(
              COMMAND_CONTEXT, findOperation, 3, DocumentProjector.identityProjector());

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);
      assertThat(deleteCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
      assertThat(result.data().getResponseDocuments().get(0).toString()).isEqualTo(docJson2);
    }

    @Test
    public void deleteWithIdNoData() {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql, keyValue);

      List<Row> rows = Arrays.asList();

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });
      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);
      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 0);
    }

    @Test
    public void deleteWithDynamic() {
      UUID tx_id = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue), byteBufferFrom(tx_id))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
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
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);
      assertThat(deleteCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
    }

    @Test
    public void deleteWithDynamicRetry() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue), byteBufferFrom(tx_id1))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      stmt =
          SimpleStatement.newInstance(
              collectionReadCql2,
              keyValue,
              "username " + new DocValueHasher().getHash("user1").hash());

      rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue), byteBufferFrom(tx_id2))));

      AsyncResultSet mockResults1 = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults1);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id2);
      deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults2 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults2);
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
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 2);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(2);
      assertThat(deleteCallCount.get()).isEqualTo(2);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
    }

    @Test
    public void deleteWithDynamicRetryFailure() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue), byteBufferFrom(tx_id1))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      stmt =
          SimpleStatement.newInstance(
              collectionReadCql2,
              keyValue,
              "username " + new DocValueHasher().getHash("user1").hash());

      rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue), byteBufferFrom(tx_id2))));

      AsyncResultSet mockResults1 = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults1);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id2);
      deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults2 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults2);
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
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 2);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(3);
      assertThat(deleteCallCount.get()).isEqualTo(3);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 0);
    }

    @Test
    public void deleteWithDynamicRetryConcurrentDelete() {
      UUID tx_id1 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final TupleValue keyValue = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue), byteBufferFrom(tx_id1))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      stmt =
          SimpleStatement.newInstance(
              collectionReadCql2,
              keyValue,
              "username " + new DocValueHasher().getHash("user1").hash());

      rows = Arrays.asList();

      AsyncResultSet mockResults1 = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults1);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
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
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 2);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(2);
      assertThat(deleteCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 0);
    }

    @Test
    public void deleteManyWithDynamic() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue1 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      final TupleValue keyValue2 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue1), byteBufferFrom(tx_id1))),
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue2), byteBufferFrom(tx_id2))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue1, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue2, tx_id2);
      deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults1 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults1);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              2,
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);
      assertThat(deleteCallCount.get()).isEqualTo(2);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 2);
    }

    @Test
    public void deleteWithNoResult() {
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue2 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows = Arrays.asList();

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
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
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 1, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 0);
    }

    @Test
    public void errorPartial() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      UUID tx_id3 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue1 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      final TupleValue keyValue2 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue1), byteBufferFrom(tx_id1))),
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue2), byteBufferFrom(tx_id2))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      stmt =
          SimpleStatement.newInstance(
              collectionReadCql2,
              keyValue1,
              "username " + new DocValueHasher().getHash("user1").hash());

      rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue1), byteBufferFrom(tx_id3))));

      AsyncResultSet mockResults2 = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults2);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue1, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue2, tx_id2);
      deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults1 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults1);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue1, tx_id3);
      deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults2 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              3,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(4);
      assertThat(deleteCallCount.get()).isEqualTo(5);

      // then result
      CommandResult result = execute.get();

      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status()).isNotNull();
                assertThat(result.status().get(CommandStatus.DELETED_COUNT)).isEqualTo(1);
                assertThat(result.errors()).isNotNull();
                assertThat(result.errors()).hasSize(1);
                assertThat(result.errors().get(0).fields().get("errorCode"))
                    .isEqualTo("CONCURRENCY_FAILURE");
                assertThat(result.errors().get(0).message())
                    .isEqualTo(
                        "Failed to delete documents with _id ['doc1']: Unable to complete transaction due to concurrent transactions");
              });
    }

    @Test
    public void errorAll() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      UUID tx_id3 = UUID.randomUUID();
      UUID tx_id4 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue1 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      final TupleValue keyValue2 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue1), byteBufferFrom(tx_id1))),
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue2), byteBufferFrom(tx_id2))));

      AsyncResultSet mockResults = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      final AtomicInteger selectCallCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      String collectionReadCql2 =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE (key = ? AND array_contains CONTAINS ?) LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      stmt =
          SimpleStatement.newInstance(
              collectionReadCql2,
              keyValue1,
              "username " + new DocValueHasher().getHash("user1").hash());

      rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue1), byteBufferFrom(tx_id3))));

      AsyncResultSet mockResults1 = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults1);
              });

      stmt =
          SimpleStatement.newInstance(
              collectionReadCql2,
              keyValue2,
              "username " + new DocValueHasher().getHash("user1").hash());

      rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue2), byteBufferFrom(tx_id4))));

      AsyncResultSet mockResults2 = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt), any(), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults2);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue1, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue2, tx_id2);
      deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults2 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults2);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue1, tx_id3);
      deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults3 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults3);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue2, tx_id4);
      deleteRows =
          Arrays.asList(
              new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(false))));

      AsyncResultSet deleteResults4 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults4);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              3,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(7);
      assertThat(deleteCallCount.get()).isEqualTo(8);

      // then result
      CommandResult result = execute.get();

      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status()).isNotNull();
                assertThat(result.status().get(CommandStatus.DELETED_COUNT)).isEqualTo(0);
                assertThat(result.errors()).isNotNull();
                assertThat(result.errors()).hasSize(1);
                assertThat(result.errors().get(0).fields().get("errorCode"))
                    .isEqualTo("CONCURRENCY_FAILURE");
                assertThat(result.errors().get(0).message())
                    .isEqualTo(
                        "Failed to delete documents with _id ['doc1', 'doc2']: Unable to complete transaction due to concurrent transactions");
              });
    }

    @Test
    public void deleteManyWithDynamicPaging() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String pagingState = "pagingState";
      ByteBuffer pagingStateBB = ByteBuffer.wrap(pagingState.getBytes());
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue1 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      final TupleValue keyValue2 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue1), byteBufferFrom(tx_id1))));
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      ExecutionInfo executionInfo = mock(ExecutionInfo.class);
      when(executionInfo.getPagingState()).thenReturn(pagingStateBB);
      final CompletableFuture<AsyncResultSet> asyncResultSetCompletableFuture =
          mock(CompletableFuture.class);
      AsyncResultSet mockResults =
          new MockAsyncResultSet(
              SELECT_RESULT_COLUMNS, rows, asyncResultSetCompletableFuture, executionInfo);
      final AtomicInteger selectCallCount = new AtomicInteger();
      when(queryExecutor.executeRead(
              eq(dataApiRequestInfo), eq(stmt), eq(Optional.empty()), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue2), byteBufferFrom(tx_id2))));

      AsyncResultSet mockResults1 = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      when(queryExecutor.executeRead(
              eq(dataApiRequestInfo),
              eq(stmt),
              eq(Optional.of(Base64.getEncoder().encodeToString(pagingStateBB.array()))),
              anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults1);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue1, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue2, tx_id2);
      deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults2 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              1,
              ReadType.KEY,
              objectMapper);
      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(selectCallCount.get()).isEqualTo(2);
      assertThat(deleteCallCount.get()).isEqualTo(2);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 2);
    }

    @Test
    public void deleteManyWithDynamicPagingAndMoreData() {
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      UUID tx_id3 = UUID.randomUUID();
      String pagingState = "pagingState";
      ByteBuffer pagingStateBB = ByteBuffer.wrap(pagingState.getBytes());

      String pagingState2 = "pagingState2";
      ByteBuffer pagingStateBB2 = ByteBuffer.wrap(pagingState2.getBytes());

      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 3"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      final TupleValue keyValue1 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"));
      final TupleValue keyValue2 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"));
      final TupleValue keyValue3 = CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc3"));
      SimpleStatement stmt =
          SimpleStatement.newInstance(
              collectionReadCql, "username " + new DocValueHasher().getHash("user1").hash());

      List<Row> rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue1), byteBufferFrom(tx_id1))));
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      ExecutionInfo executionInfo = mock(ExecutionInfo.class);
      when(executionInfo.getPagingState()).thenReturn(pagingStateBB);
      CompletableFuture<AsyncResultSet> asyncResultSetCompletableFuture =
          mock(CompletableFuture.class);
      AsyncResultSet mockResults =
          new MockAsyncResultSet(
              SELECT_RESULT_COLUMNS, rows, asyncResultSetCompletableFuture, executionInfo);
      final AtomicInteger selectCallCount = new AtomicInteger();
      when(queryExecutor.executeRead(
              eq(dataApiRequestInfo), eq(stmt), eq(Optional.empty()), anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults);
              });

      rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue2), byteBufferFrom(tx_id2))));
      executionInfo = mock(ExecutionInfo.class);
      when(executionInfo.getPagingState()).thenReturn(pagingStateBB2);
      AsyncResultSet mockResults1 =
          new MockAsyncResultSet(
              SELECT_RESULT_COLUMNS, rows, asyncResultSetCompletableFuture, executionInfo);
      when(queryExecutor.executeRead(
              eq(dataApiRequestInfo),
              eq(stmt),
              eq(Optional.of(Base64.getEncoder().encodeToString(pagingStateBB.array()))),
              anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults1);
              });

      rows =
          Arrays.asList(
              new MockRow(
                  SELECT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(byteBufferFrom(keyValue3), byteBufferFrom(tx_id3))));
      AsyncResultSet mockResults2 = new MockAsyncResultSet(SELECT_RESULT_COLUMNS, rows, null);
      when(queryExecutor.executeRead(
              eq(dataApiRequestInfo),
              eq(stmt),
              eq(Optional.of(Base64.getEncoder().encodeToString(pagingStateBB2.array()))),
              anyInt()))
          .then(
              invocation -> {
                selectCallCount.incrementAndGet();
                return Uni.createFrom().item(mockResults2);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, keyValue1, tx_id1);
      List<Row> deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);
      final AtomicInteger deleteCallCount = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      deleteStmt = SimpleStatement.newInstance(collectionDeleteCql, keyValue2, tx_id2);
      deleteRows =
          Arrays.asList(new MockRow(DELETE_RESULT_COLUMNS, 0, Arrays.asList(byteBufferFrom(true))));

      AsyncResultSet deleteResults2 =
          new MockAsyncResultSet(DELETE_RESULT_COLUMNS, deleteRows, null);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                deleteCallCount.incrementAndGet();
                return Uni.createFrom().item(deleteResults2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              3,
              1,
              ReadType.KEY,
              objectMapper);

      DeleteOperation operation = DeleteOperation.delete(COMMAND_CONTEXT, findOperation, 2, 3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(selectCallCount.get()).isEqualTo(3);
      assertThat(deleteCallCount.get()).isEqualTo(2);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.DELETED_COUNT, 2)
          .containsEntry(CommandStatus.MORE_DATA, true);
    }
  }
}
