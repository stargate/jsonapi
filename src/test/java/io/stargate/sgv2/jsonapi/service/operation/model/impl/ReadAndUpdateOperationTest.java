package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperator;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class ReadAndUpdateOperationTest extends OperationTestBase {
  private CommandContext COMMAND_CONTEXT;

  private CommandContext COMMAND_VECTOR_CONTEXT;

  @Inject Shredder shredder;
  @Inject ObjectMapper objectMapper;

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

  private static String UPDATE_VECTOR =
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
          + "            query_vector_value = ?,"
          + "            doc_json  = ?"
          + "        WHERE "
          + "            key = ?"
          + "        IF "
          + "            tx_id = ?";

  private final ColumnDefinitions KEY_TXID_JSON_COLUMNS =
      buildColumnDefs(
          TestColumn.keyColumn(), TestColumn.ofUuid("tx_id"), TestColumn.ofVarchar("doc_json"));

  @PostConstruct
  public void init() {
    COMMAND_CONTEXT =
        new CommandContext(
            KEYSPACE_NAME, COLLECTION_NAME, "testCommand", jsonProcessingMetricsReporter);
    COMMAND_VECTOR_CONTEXT =
        new CommandContext(
            KEYSPACE_NAME,
            COLLECTION_NAME,
            new CollectionSettings(
                COLLECTION_NAME,
                true,
                -1,
                CollectionSettings.SimilarityFunction.COSINE,
                null,
                null,
                null),
            null,
            "testCommand",
            jsonProcessingMetricsReporter);
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
      buildColumnDefs(TestColumn.ofBoolean("[applied]"));

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

  private SimpleStatement vectorUpdateStatement(
      WritableShreddedDocument shredDocument, UUID tx_id) {
    String updateCql = UPDATE_VECTOR.formatted(KEYSPACE_NAME, COLLECTION_NAME);
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
        CQLBindValues.getVectorValue(shredDocument.queryVectorValues()),
        shredDocument.docJson(),
        CQLBindValues.getDocumentIdValue(shredDocument.id()),
        tx_id);
  }

  @Nested
  class UpdateOne {
    @Test
    public void happyPath() throws Exception {
      UUID tx_id = UUID.randomUUID();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
            {
              "_id": "doc1",
              "username": "user1"
            }
            """;
      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", tx_id, doc1));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
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
              COMMAND_VECTOR_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      // update
      String doc1Updated =
          """
            {
              "_id": "doc1",
              "username": "user1",
              "$vector" : [0.11,0.22,0.33,0.44]
            }
            """;

      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt2 = vectorUpdateStatement(shredDocument, tx_id);
      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      String updateClause =
          """
                   { "$set" : { "$vector" : [0.11,0.22,0.33,0.44] }}
              """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(objectMapper.readValue(updateClause, UpdateClause.class));
      ReadAndUpdateOperation operation =
          new ReadAndUpdateOperation(
              COMMAND_VECTOR_CONTEXT,
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
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
      assertThat(result.errors()).isNull();
    }

    @Test
    public void noChange() throws Exception {
      UUID tx_id = UUID.randomUUID();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                {
                  "_id": "doc1",
                  "username": "user1"
                }
                """;
      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", tx_id, doc1));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      CommandContext commandContext = createCommandContextWithCommandName("ReadNoWriteCommand");

      FindOperation findOperation =
          FindOperation.unsortedSingle(
              commandContext,
              implicitAnd,
              DocumentProjector.identityProjector(),
              ReadType.DOCUMENT,
              objectMapper);

      // update
      String updateClause =
          """
                      { "$set" : { "username" : "user1" }}
                  """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(objectMapper.readValue(updateClause, UpdateClause.class));
      ReadAndUpdateOperation operation =
          new ReadAndUpdateOperation(
              commandContext,
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
      assertThat(callCount1.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 0);
      assertThat(result.errors()).isNull();
      assertThat(result.data().getResponseDocuments()).hasSize(1);

      // verify metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();

      // there should be no json_bytes_written metrics (no write)
      List<String> jsonBytesWriteMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_bytes_written") && line.contains("ReadNoWriteCommand"))
              .toList();
      assertThat(jsonBytesWriteMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(0);
              });
      // should have three metrics start with "json_docs_written" in total
      List<String> jsonDocsWrittenMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written")
                          && !line.startsWith("json_docs_written_bucket")
                          && !line.contains("quantile")
                          && line.contains("ReadNoWriteCommand"))
              .toList();
      assertThat(jsonDocsWrittenMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"ReadNoWriteCommand\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");
                    });
              });
      // verify count metric -- command called once, the value should be one
      List<String> jsonDocsWrittenCountMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written_count")
                          && line.contains("ReadNoWriteCommand"))
              .toList();
      assertThat(jsonDocsWrittenCountMetrics).hasSize(1);
      jsonDocsWrittenCountMetrics.forEach(
          line -> {
            System.out.println(line);
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(1.0);
          });
      // verify sum metric -- update no docs, should be zero
      List<String> jsonDocsWrittenSumMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written_sum")
                          && line.contains("ReadNoWriteCommand"))
              .toList();
      assertThat(jsonDocsWrittenSumMetrics).hasSize(1);
      jsonDocsWrittenSumMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(0.0);
          });

      // there should be three json_bytes_read metrics (one read)
      List<String> jsonBytesReadMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_bytes_read")
                          && !line.startsWith("json_bytes_read_bucket")
                          && !line.contains("quantile")
                          && line.contains("ReadNoWriteCommand"))
              .toList();
      // should have three metrics in total
      assertThat(jsonBytesReadMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"ReadNoWriteCommand\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");
                    });
              });
      // verify count metric -- command called once, the value should be one
      List<String> jsonDocsReadCountMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_read_count")
                          && line.contains("ReadNoWriteCommand"))
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
                      line.startsWith("json_docs_read_sum") && line.contains("ReadNoWriteCommand"))
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
    public void happyPathWithSort() throws Exception {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 10000"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
              {
                "_id": "doc1",
                "username": "user1",
                "filter_me" : "happy"
              }
              """;

      String doc2 =
          """
              {
                "_id": "doc2",
                "username": "user2",
                "filter_me" : "happy"
              }
              """;

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

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(
              collectionReadCql, "filter_me " + new DocValueHasher().getHash("happy").hash());
      List<Row> rows1 =
          Arrays.asList(
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(
                          CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"))),
                      byteBufferFrom(tx_id1),
                      byteBufferFrom(doc1),
                      byteBufferFrom("user1"),
                      null,
                      null,
                      null,
                      null)),
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(
                          CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"))),
                      byteBufferFrom(tx_id2),
                      byteBufferFrom(doc2),
                      byteBufferFrom("user2"),
                      null,
                      null,
                      null,
                      null)));
      AsyncResultSet results1 = new MockAsyncResultSet(SELECT_SORT_RESULT_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });

      // update
      String doc1Updated =
          """
              {
                "_id": "doc1",
                "username": "user1",
                "filter_me" : "happy",
                "name" : "test"
              }
              """;
      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt2 = nonVectorUpdateStatement(shredDocument, tx_id1);

      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "filter_me", DBFilterBase.MapFilterBase.Operator.EQ, "happy"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      CommandContext commandContext = createCommandContextWithCommandName("ReadAndWriteCommand");

      FindOperation findOperation =
          FindOperation.sorted(
              commandContext,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              1,
              100,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              0,
              10000);

      DocumentUpdater documentUpdater =
          DocumentUpdater.construct(
              DocumentUpdaterUtils.updateClause(
                  UpdateOperator.SET, objectMapper.createObjectNode().put("name", "test")));
      ReadAndUpdateOperation operation =
          new ReadAndUpdateOperation(
              commandContext,
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
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
      assertThat(result.errors()).isNull();

      // verify metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();

      // there should be three json_bytes_written metrics (one write)
      List<String> jsonBytesWriteMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_bytes_written")
                          && !line.startsWith("json_bytes_written_bucket")
                          && !line.contains("quantile")
                          && line.contains("ReadAndWriteCommand"))
              .toList();
      assertThat(jsonBytesWriteMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"ReadAndWriteCommand\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                      assertThat(line).contains("tenant=\"unknown\"");
                    });
              });
      // verify count metric -- command called once, the value should be one
      List<String> jsonDocsWrittenCountMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written_count")
                          && line.contains("ReadAndWriteCommand"))
              .toList();
      assertThat(jsonDocsWrittenCountMetrics).hasSize(1);
      jsonDocsWrittenCountMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(1.0);
          });
      // verify sum metric -- update one docs, should be one
      List<String> jsonDocsWrittenSumMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written_sum")
                          && line.contains("ReadAndWriteCommand"))
              .toList();
      assertThat(jsonDocsWrittenSumMetrics).hasSize(1);
      jsonDocsWrittenSumMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(1.0);
          });

      // there should be three json_bytes_read metrics (one read)
      List<String> jsonBytesReadMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_bytes_read")
                          && !line.startsWith("json_bytes_read_bucket")
                          && !line.contains("quantile")
                          && line.contains("ReadAndWriteCommand"))
              .toList();
      assertThat(jsonBytesReadMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"ReadAndWriteCommand\"");
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
                          && line.contains("ReadAndWriteCommand"))
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
                      line.startsWith("json_docs_read_sum") && line.contains("ReadAndWriteCommand"))
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
    public void happyPathReplace() throws Exception {
      UUID tx_id = UUID.randomUUID();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
                {
                  "_id": "doc1",
                  "username": "user1"
                }
                """;

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList(resultRow(0, "doc1", tx_id, doc1));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
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
              ReadType.DOCUMENT,
              objectMapper);

      // update
      String doc1Updated =
          """
                {
                  "_id": "doc1",
                  "name" : "test"
                }
                """;
      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      SimpleStatement stmt2 = nonVectorUpdateStatement(shredDocument, tx_id);
      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      String replacement =
          """
                  {
                    "name" : "test"
                  }
                  """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct((ObjectNode) objectMapper.readTree(replacement));
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
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
      assertThat(result.errors()).isNull();
    }

    @Test
    public void happyPathReplaceUpsert() throws Exception {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1_select_update =
          """
                {
                  "_id": "doc1",
                  "name": "test"
                }
                """;

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList();
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
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
              ReadType.DOCUMENT,
              objectMapper);

      // update
      JsonNode jsonNode = objectMapper.readTree(doc1_select_update);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt2 = nonVectorUpdateStatement(shredDocument, null);
      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      String replacement =
          """
                  {
                    "name" : "test"
                  }
                  """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct((ObjectNode) objectMapper.readTree(replacement));
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
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(3)
          .containsEntry(CommandStatus.MATCHED_COUNT, 0)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 0)
          .containsEntry(CommandStatus.UPSERTED_ID, new DocumentId.StringId("doc1"));
      assertThat(result.errors()).isNull();
    }

    @Test
    public void happyPathReplaceWithSort() throws Exception {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 10000"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
              {
                "_id": "doc1",
                "username": "user1",
                "filter_me" : "happy"
              }
              """;

      String doc2 =
          """
              {
                "_id": "doc2",
                "username": "user2",
                "filter_me" : "happy"
              }
              """;
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

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(
              collectionReadCql, "filter_me " + new DocValueHasher().getHash("happy").hash());
      List<Row> rows1 =
          Arrays.asList(
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(
                          CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"))),
                      byteBufferFrom(tx_id1),
                      byteBufferFrom(doc1),
                      byteBufferFrom("user1"),
                      null,
                      null,
                      null,
                      null)),
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(
                          CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"))),
                      byteBufferFrom(tx_id2),
                      byteBufferFrom(doc2),
                      byteBufferFrom("user2"),
                      null,
                      null,
                      null,
                      null)));
      AsyncResultSet results1 = new MockAsyncResultSet(SELECT_SORT_RESULT_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });

      // update
      String doc1Updated =
          """
              {
                "_id": "doc1",
                "username": "user1",
                "filter_me" : "happy",
                "name" : "test"
              }
              """;

      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt2 = nonVectorUpdateStatement(shredDocument, tx_id1);

      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "filter_me", DBFilterBase.MapFilterBase.Operator.EQ, "happy"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.sortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              100,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", true)),
              0,
              10000);

      String replacement =
          """
              {
                "username": "user1",
                "filter_me" : "happy",
                "name" : "test"
              }
              """;
      DocumentUpdater documentUpdater =
          DocumentUpdater.construct((ObjectNode) objectMapper.readTree(replacement));
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
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
      assertThat(result.errors()).isNull();
    }

    @Test
    public void happyPathWithSortDescending() throws Exception {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String collectionReadCql =
          "SELECT key, tx_id, doc_json, query_text_values['username'], query_dbl_values['username'], query_bool_values['username'], query_null_values['username'], query_timestamp_values['username'] FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 10000"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);
      String doc1 =
          """
              {
                "_id": "doc1",
                "username": "user1",
                "filter_me" : "happy"
              }
              """;

      String doc2 =
          """
              {
                "_id": "doc2",
                "username": "user2",
                "filter_me" : "happy"
              }
              """;

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

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(
              collectionReadCql, "filter_me " + new DocValueHasher().getHash("happy").hash());
      List<Row> rows1 =
          Arrays.asList(
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(
                          CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc1"))),
                      byteBufferFrom(tx_id1),
                      byteBufferFrom(doc1),
                      byteBufferFrom("user1"),
                      null,
                      null,
                      null,
                      null)),
              new MockRow(
                  SELECT_SORT_RESULT_COLUMNS,
                  0,
                  Arrays.asList(
                      byteBufferFrom(
                          CQLBindValues.getDocumentIdValue(DocumentId.fromString("doc2"))),
                      byteBufferFrom(tx_id2),
                      byteBufferFrom(doc2),
                      byteBufferFrom("user2"),
                      null,
                      null,
                      null,
                      null)));
      AsyncResultSet results1 = new MockAsyncResultSet(SELECT_SORT_RESULT_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });

      // update
      String doc2Updated =
          """
                {
                  "_id": "doc2",
                  "username": "user2",
                  "filter_me" : "happy",
                  "name" : "test"
                }
                """;
      JsonNode jsonNode = objectMapper.readTree(doc2Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt2 = nonVectorUpdateStatement(shredDocument, tx_id2);

      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.TextFilter(
                  "filter_me", DBFilterBase.MapFilterBase.Operator.EQ, "happy"));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.sortedSingle(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              100,
              ReadType.SORTED_DOCUMENT,
              objectMapper,
              List.of(new FindOperation.OrderBy("username", false)),
              0,
              10000);

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
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 1)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 1);
      assertThat(result.errors()).isNull();
    }

    @Test
    public void withUpsert() throws Exception {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList();
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
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
              ReadType.DOCUMENT,
              objectMapper);

      // update
      String doc1Updated =
          """
                {
                  "_id": "doc1",
                  "name" : "test"
                }
                """;

      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt2 = nonVectorUpdateStatement(shredDocument, null);
      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

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
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(3)
          .containsEntry(CommandStatus.MATCHED_COUNT, 0)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 0)
          .containsEntry(CommandStatus.UPSERTED_ID, new DocumentId.StringId("doc1"));
      assertThat(result.errors()).isNull();
    }

    @Test
    public void noData() {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 1"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList();
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
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
              ReadType.DOCUMENT,
              objectMapper);

      // update
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
      assertThat(callCount1.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 0)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 0);
      assertThat(result.errors()).isNull();
    }
  }

  @Nested
  class UpdateMany {

    @Test
    public void happyPath() throws Exception {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 21"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      UUID tx_id1 = UUID.randomUUID();
      UUID tx_id2 = UUID.randomUUID();
      String doc1 =
          """
                      {
                        "_id": "doc1",
                        "username": "user1",
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

      String doc2 =
          """
                      {
                        "_id": "doc2",
                        "username": "user2",
                        "status" : "active"
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
          Arrays.asList(resultRow(0, "doc1", tx_id1, doc1), resultRow(0, "doc2", tx_id2, doc2));
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });

      // update
      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt2 = nonVectorUpdateStatement(shredDocument, tx_id1);
      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      jsonNode = objectMapper.readTree(doc2Updated);
      shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt3 = nonVectorUpdateStatement(shredDocument, tx_id2);
      List<Row> rows3 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results3 = new MockAsyncResultSet(COLUMNS_APPLIED, rows3, null);
      final AtomicInteger callCount3 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt3)))
          .then(
              invocation -> {
                callCount3.incrementAndGet();
                return Uni.createFrom().item(results3);
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
              21,
              20,
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
              20,
              3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);
      assertThat(callCount3.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 2)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 2);
      assertThat(result.errors()).isNull();
    }

    @Test
    public void withUpsert() throws Exception {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE key = ? LIMIT 21"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(collectionReadCql, boundKeyForStatement("doc1"));
      List<Row> rows1 = Arrays.asList();
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
              });

      // update
      String doc1Updated =
          """
                  {
                    "_id": "doc1",
                    "name" : "test"
                  }
                  """;
      JsonNode jsonNode = objectMapper.readTree(doc1Updated);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      SimpleStatement stmt2 = nonVectorUpdateStatement(shredDocument, null);
      List<Row> rows2 = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows2, null);
      final AtomicInteger callCount2 = new AtomicInteger();
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(stmt2)))
          .then(
              invocation -> {
                callCount2.incrementAndGet();
                return Uni.createFrom().item(results2);
              });

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      List<DBFilterBase> filters =
          List.of(
              new DBFilterBase.IDFilter(
                  DBFilterBase.IDFilter.Operator.EQ, DocumentId.fromString("doc1")));
      implicitAnd.comparisonExpressions.get(0).setDBFilters(filters);

      FindOperation findOperation =
          FindOperation.unsorted(
              COMMAND_CONTEXT,
              implicitAnd,
              DocumentProjector.identityProjector(),
              null,
              21,
              20,
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
              20,
              3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount2.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(3)
          .containsEntry(CommandStatus.MATCHED_COUNT, 0)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 0)
          .containsEntry(CommandStatus.UPSERTED_ID, new DocumentId.StringId("doc1"));
      assertThat(result.errors()).isNull();
    }

    @Test
    public void noData() {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      // read
      String collectionReadCql =
          "SELECT key, tx_id, doc_json FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ? LIMIT 21"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      SimpleStatement stmt1 =
          SimpleStatement.newInstance(
              collectionReadCql, "status " + new DocValueHasher().getHash("active").hash());
      List<Row> rows1 = Arrays.asList();
      AsyncResultSet results1 = new MockAsyncResultSet(KEY_TXID_JSON_COLUMNS, rows1, null);
      final AtomicInteger callCount1 = new AtomicInteger();
      when(queryExecutor.executeRead(eq(dataApiRequestInfo), eq(stmt1), any(), anyInt()))
          .then(
              invocation -> {
                callCount1.incrementAndGet();
                return Uni.createFrom().item(results1);
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
              21,
              20,
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
              20,
              3);

      Supplier<CommandResult> execute =
          operation
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount1.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(2)
          .containsEntry(CommandStatus.MATCHED_COUNT, 0)
          .containsEntry(CommandStatus.MODIFIED_COUNT, 0);
      assertThat(result.errors()).isNull();
    }
  }
}
