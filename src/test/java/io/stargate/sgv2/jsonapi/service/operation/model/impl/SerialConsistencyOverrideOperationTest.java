package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
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
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
// import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.testutil.DocumentUpdaterUtils;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(SerialConsistencyOverrideOperationTest.SerialConsistencyOverrideProfile.class)
public class SerialConsistencyOverrideOperationTest extends OperationTestBase {
  private CommandContext COMMAND_CONTEXT;
  private final ColumnDefinitions COLUMNS_APPLIED =
      buildColumnDefs(TestColumn.ofBoolean("[applied]"));

  @Inject ObjectMapper objectMapper;
  @Inject Shredder shredder;

  public static class SerialConsistencyOverrideProfile implements QuarkusTestProfile {
    @Override
    public boolean disableGlobalTestResources() {
      return true;
    }

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .put("stargate.queries.serial-consistency", "LOCAL_SERIAL")
          .build();
    }
  }

  @PostConstruct
  public void init() {
    COMMAND_CONTEXT =
        new CommandContext(
            KEYSPACE_NAME, COLLECTION_NAME, "testCommand", jsonProcessingMetricsReporter);
  }

  @Nested
  class Delete {
    @Test
    public void delete() {
      UUID tx_id = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      final ColumnDefinitions keyAndTxtIdColumns =
          buildColumnDefs(TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"));
      SimpleStatement selectStmt =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> selectRows =
          Arrays.asList(resultRow(keyAndTxtIdColumns, 0, byteBufferForKey("doc1"), tx_id));
      AsyncResultSet selectResults = new MockAsyncResultSet(keyAndTxtIdColumns, selectRows, null);
      final AtomicInteger callCountSelect = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(selectStmt), any(), anyInt()))
          .then(
              invocation -> {
                callCountSelect.incrementAndGet();
                return Uni.createFrom().item(selectResults);
              });

      String collectionDeleteCql =
          "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement deleteStmt =
          SimpleStatement.newInstance(collectionDeleteCql, boundKeyForStatement("doc1"), tx_id);
      List<Row> deleteRows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, byteBufferFrom(true)));
      AsyncResultSet deleteResults = new MockAsyncResultSet(COLUMNS_APPLIED, deleteRows, null);
      final AtomicInteger callCountDelete = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(deleteStmt)))
          .then(
              invocation -> {
                SimpleStatement stmt = invocation.getArgument(1);
                callCountDelete.incrementAndGet();
                return Uni.createFrom().item(deleteResults);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters1 =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters1);

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
      assertThat(callCountSelect.get()).isEqualTo(1);
      assertThat(callCountDelete.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.DELETED_COUNT, 1);
    }
  }

  @Nested
  class Insert {
    static final String INSERT_CQL =
        "INSERT INTO \"%s\".\"%s\""
            + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values)"
            + " VALUES"
            + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  IF NOT EXISTS";

    @Test
    public void insert() throws Exception {
      String document =
          """
              {
                "_id": "doc1",
                "text": "user1",
                "number" : 10,
                "boolean": true,
                "nullval" : null,
                "array" : ["a", "b"],
                "sub_doc" : {"col": "val"}
              }
              """;

      JsonNode jsonNode = objectMapper.readTree(document);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      SimpleStatement stmt =
          SimpleStatement.newInstance(
              INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME),
              CQLBindValues.getDocumentIdValue(shredDocument.id()),
              shredDocument.nextTxID(),
              shredDocument.docJson(),
              CQLBindValues.getSetValue(shredDocument.existKeys()),
              CQLBindValues.getIntegerMapValues(shredDocument.arraySize()),
              shredDocument.arrayContains(),
              CQLBindValues.getBooleanMapValues(shredDocument.queryBoolValues()),
              CQLBindValues.getDoubleMapValues(shredDocument.queryNumberValues()),
              CQLBindValues.getStringMapValues(shredDocument.queryTextValues()),
              CQLBindValues.getSetValue(shredDocument.queryNullValues()),
              CQLBindValues.getTimestampMapValues(shredDocument.queryTimestampValues()));

      List<Row> resultRows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, byteBufferFrom(true)));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED, resultRows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      InsertOperation operation = new InsertOperation(COMMAND_CONTEXT, shredDocument);
      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors()).isNull();
    }
  }

  @Nested
  class ReadAndUpdate {

    @Test
    public void readAndUpdate() throws Exception {
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      UUID tx_id = UUID.randomUUID();
      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1"
                      }
                      """;

      final ColumnDefinitions keyTxIdDocColumns =
          buildColumnDefs(
              TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"), TestColumn.ofVarchar("doc_json"));
      SimpleStatement selectStmt =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> selectRows =
          Arrays.asList(resultRow(keyTxIdDocColumns, 0, byteBufferForKey("doc1"), tx_id, doc1));
      AsyncResultSet selectResults = new MockAsyncResultSet(keyTxIdDocColumns, selectRows, null);
      final AtomicInteger callCountSelect = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(selectStmt), any(), anyInt()))
          .then(
              invocation -> {
                callCountSelect.incrementAndGet();
                return Uni.createFrom().item(selectResults);
              });

      String doc1Updated =
          """
                          {
                            "_id": "doc1",
                            "username": "user1",
                            "name" : "test"
                          }
                          """;

      String update =
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
      String updateCql = update.formatted(KEYSPACE_NAME, COLLECTION_NAME);
      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      SimpleStatement updateStmt =
          SimpleStatement.newInstance(
              updateCql.formatted(KEYSPACE_NAME, COLLECTION_NAME),
              CQLBindValues.getSetValue(shredDocument.existKeys()),
              CQLBindValues.getIntegerMapValues(shredDocument.arraySize()),
              shredDocument.arrayContains(),
              CQLBindValues.getBooleanMapValues(shredDocument.queryBoolValues()),
              CQLBindValues.getDoubleMapValues(shredDocument.queryNumberValues()),
              CQLBindValues.getStringMapValues(shredDocument.queryTextValues()),
              CQLBindValues.getSetValue(shredDocument.queryNullValues()),
              CQLBindValues.getTimestampMapValues(shredDocument.queryTimestampValues()),
              shredDocument.docJson(),
              CQLBindValues.getDocumentIdValue(shredDocument.id()),
              tx_id);

      List<Row> resultRows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, byteBufferFrom(true)));
      AsyncResultSet updateResults = new MockAsyncResultSet(COLUMNS_APPLIED, resultRows, null);
      final AtomicInteger callCountUpdate = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(updateStmt)))
          .then(
              invocation -> {
                callCountUpdate.incrementAndGet();
                return Uni.createFrom().item(updateResults);
              });

      DBFilterBase.IDFilter filter =
          new DBFilterBase.IDFilter(
              DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1"));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters1 = List.of(filter);
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters1);

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
      assertThat(callCountSelect.get()).isEqualTo(1);
      assertThat(callCountUpdate.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
      assertThat(result.errors()).isNull();
    }
  }

  private MockRow resultRow(ColumnDefinitions columnDefs, int index, Object... values) {
    List<ByteBuffer> buffers = Stream.of(values).map(value -> byteBufferFromAny(value)).toList();
    return new MockRow(columnDefs, index, buffers);
  }
}
