package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
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
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
public class InsertOperationTest extends OperationTestBase {
  private CommandContext COMMAND_CONTEXT_NON_VECTOR;

  private CommandContext COMMAND_CONTEXT_VECTOR;

  private final ColumnDefinitions COLUMNS_APPLIED =
      buildColumnDefs(TestColumn.ofBoolean("[applied]"));

  private final ColumnDefinitions COLUMNS_APPLIED_FAILURE =
      buildColumnDefs(TestColumn.ofBoolean("[applied]"), TestColumn.ofUuid("tx_id"));

  @Inject Shredder shredder;
  @Inject ObjectMapper objectMapper;

  static final String INSERT_CQL =
      "INSERT INTO \"%s\".\"%s\""
          + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values)"
          + " VALUES"
          + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  IF NOT EXISTS";

  static final String INSERT_VECTOR_CQL =
      "INSERT INTO \"%s\".\"%s\""
          + " (key, tx_id, doc_json, exist_keys, array_size, array_contains, query_bool_values, query_dbl_values , query_text_values, query_null_values, query_timestamp_values, query_vector_value)"
          + " VALUES"
          + " (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  IF NOT EXISTS";

  @PostConstruct
  public void init() {
    COMMAND_CONTEXT_NON_VECTOR =
        new CommandContext(
            KEYSPACE_NAME, COLLECTION_NAME, "testCommand", jsonProcessingMetricsReporter);
    COMMAND_CONTEXT_VECTOR =
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

  @Nested
  class InsertNonVector {
    @Test
    public void insertOne() throws Exception {
      String document =
          """
                          {
                            "_id": "doc1",
                            "text": "user1",
                            "number" : 10,
                            "boolean": true,
                            "nullval" : null,
                            "array" : ["a", "b"],
                            "sub_doc" : {"col": "val"},
                            "date_val" : {"$date": 1672531200000 }
                          }
                          """;

      JsonNode jsonNode = objectMapper.readTree(document);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      SimpleStatement insertStmt = nonVectorInsertStatement(shredDocument);
      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertOperation(COMMAND_CONTEXT_NON_VECTOR, shredDocument)
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

    @Test
    public void insertDuplicate() throws Exception {
      String doc1 =
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

      final JsonNode jsonNode = objectMapper.readTree(doc1);
      final WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      SimpleStatement insertStmt = nonVectorInsertStatement(shredDocument);
      // Note: FALSE is needed to "fail" insertion, producing failure message
      List<Row> rows =
          Arrays.asList(resultRow(COLUMNS_APPLIED_FAILURE, 0, Boolean.FALSE, UUID.randomUUID()));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED_FAILURE, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertOperation(COMMAND_CONTEXT_NON_VECTOR, shredDocument)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo(
                        "Failed to insert document with _id 'doc1': Document already exists with the given _id");
                assertThat(error.fields())
                    .containsEntry("exceptionClass", "JsonApiException")
                    .containsEntry("errorCode", "DOCUMENT_ALREADY_EXISTS");
              });
    }

    @Test
    public void insertManyOrdered() throws Exception {
      String document1 =
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
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(shredDocument1);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(shredDocument2);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results1 = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final List<Integer> calls = new ArrayList<>();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                calls.add(1);
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                calls.add(2);
                return Uni.createFrom().item(results2);
              });

      CommandContext commandContext =
          createCommandContextWithCommandName("jsonDocsWrittenInsertManyCommand");
      Supplier<CommandResult> execute =
          new InsertOperation(commandContext, List.of(shredDocument1, shredDocument2), true)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(calls).isEqualTo(Arrays.asList(1, 2));

      // then result
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(
              CommandStatus.INSERTED_IDS,
              List.of(new DocumentId.StringId("doc1"), new DocumentId.StringId("doc2")));
      assertThat(result.errors()).isNull();

      // verify metrics
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> jsonDocsWrittenMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written")
                          && !line.startsWith("json_docs_written_bucket")
                          && !line.contains("quantile")
                          && line.contains("jsonDocsWrittenInsertManyCommand"))
              .toList();
      // should have three metrics in total
      assertThat(jsonDocsWrittenMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"jsonDocsWrittenInsertManyCommand\"");
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
                          && line.contains("jsonDocsWrittenInsertManyCommand"))
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
      // verify sum metric -- insert two docs, should be two
      List<String> jsonDocsWrittenSumMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("json_docs_written_sum")
                          && line.contains("jsonDocsWrittenInsertManyCommand"))
              .toList();
      assertThat(jsonDocsWrittenSumMetrics).hasSize(1);
      jsonDocsWrittenSumMetrics.forEach(
          line -> {
            String[] parts = line.split(" ");
            String numericPart =
                parts[parts.length - 1]; // Get the last part which should be the number
            double value = Double.parseDouble(numericPart);
            assertThat(value).isEqualTo(2.0);
          });
    }

    @Test
    public void insertOneRetryLWTCheck() throws Exception {
      String document =
          """
                                  {
                                    "_id": "doc1",
                                    "text": "user1",
                                    "number" : 10,
                                    "boolean": true,
                                    "nullval" : null,
                                    "array" : ["a", "b"],
                                    "sub_doc" : {"col": "val"},
                                    "date_val" : {"$date": 1672531200000 }
                                  }
                                  """;

      JsonNode jsonNode = objectMapper.readTree(document);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      SimpleStatement insertStmt = nonVectorInsertStatement(shredDocument);
      List<Row> rows =
          Arrays.asList(
              resultRow(COLUMNS_APPLIED_FAILURE, 0, Boolean.FALSE, shredDocument.nextTxID()));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED_FAILURE, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertOperation(COMMAND_CONTEXT_NON_VECTOR, shredDocument)
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

    @Test
    public void insertManyUnordered() throws Exception {
      String document1 =
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
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(shredDocument1);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(shredDocument2);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results1 = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      AsyncResultSet results2 = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().item(results1);
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().item(results2);
              });

      Supplier<CommandResult> execute =
          new InsertOperation(
                  COMMAND_CONTEXT_NON_VECTOR, List.of(shredDocument1, shredDocument2), false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      assertThat(callCount.get()).isEqualTo(2);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1);
      assertThat(result.status().get(CommandStatus.INSERTED_IDS))
          .asList()
          .containsExactlyInAnyOrder(
              new DocumentId.StringId("doc1"), new DocumentId.StringId("doc2"));
      assertThat(result.errors()).isNull();
    }

    // failure modes

    @Test
    public void failureOrdered() throws Exception {
      // ordered first insert fails
      String document1 =
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
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(shredDocument1);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(shredDocument2);

      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Test break #1"));
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().item(Arrays.asList());
              });

      Supplier<CommandResult> execute =
          new InsertOperation(
                  COMMAND_CONTEXT_NON_VECTOR, List.of(shredDocument1, shredDocument2), true)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      // second query never executed
      assertThat(callCount.get()).isEqualTo(1);

      // then result
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo("Failed to insert document with _id 'doc1': Test break #1");
                assertThat(error.fields()).containsEntry("exceptionClass", "RuntimeException");
              });
    }

    @Test
    public void failureOrderedLastFails() throws Exception {
      // ordered first insert OK, second fail
      String document1 =
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
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(shredDocument1);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(shredDocument2);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet resultOk = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);

      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().item(resultOk);
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Test break #2"));
              });

      Supplier<CommandResult> execute =
          new InsertOperation(
                  COMMAND_CONTEXT_NON_VECTOR, List.of(shredDocument1, shredDocument2), true)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution: both executed (second failed)
      assertThat(callCount.get()).isEqualTo(2);

      // then result contains both insert and error
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc1")));
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo("Failed to insert document with _id 'doc2': Test break #2");
                assertThat(error.fields()).containsEntry("exceptionClass", "RuntimeException");
              });
    }

    @Test
    public void failureUnorderedPartial() throws Exception {
      // unordered one query fail
      String document1 =
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
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);
      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(shredDocument1);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(shredDocument2);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet resultOk = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);

      final AtomicInteger callCount1 = new AtomicInteger();
      final AtomicInteger callCount2 = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount1.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Test break #1"));
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount2.addAndGet(1);
                return Uni.createFrom().item(resultOk);
              });

      Supplier<CommandResult> execute =
          new InsertOperation(
                  COMMAND_CONTEXT_NON_VECTOR, List.of(shredDocument1, shredDocument2), false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution: both called
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount1.get()).isEqualTo(1);

      // then result has both insert id and errors
      CommandResult result = execute.get();
      assertThat(result.status())
          .hasSize(1)
          .containsEntry(CommandStatus.INSERTED_IDS, List.of(new DocumentId.StringId("doc2")));
      assertThat(result.errors())
          .singleElement()
          .satisfies(
              error -> {
                assertThat(error.message())
                    .isEqualTo("Failed to insert document with _id 'doc1': Test break #1");
                assertThat(error.fields()).containsEntry("exceptionClass", "RuntimeException");
              });
    }

    @Test
    public void failureUnorderedAll() throws Exception {
      // unordered both queries fail
      String document1 =
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
      String document2 =
          """
                          {
                            "_id": "doc2",
                            "text": "user2",
                            "number" : 11,
                            "boolean": false,
                            "nullval" : null,
                            "array" : ["c", "d"],
                            "sub_doc" : {"col": "lav"}
                          }
                          """;

      JsonNode jsonNode1 = objectMapper.readTree(document1);
      WritableShreddedDocument shredDocument1 = shredder.shred(jsonNode1);

      JsonNode jsonNode2 = objectMapper.readTree(document2);
      WritableShreddedDocument shredDocument2 = shredder.shred(jsonNode2);

      SimpleStatement insertStmt1 = nonVectorInsertStatement(shredDocument1);
      SimpleStatement insertStmt2 = nonVectorInsertStatement(shredDocument2);

      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet resultOk = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);

      final AtomicInteger callCount1 = new AtomicInteger();
      final AtomicInteger callCount2 = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt1)))
          .then(
              invocation -> {
                callCount1.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Insert 1 failed"));
              });
      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt2)))
          .then(
              invocation -> {
                callCount2.addAndGet(1);
                return Uni.createFrom().failure(new RuntimeException("Insert 2 failed"));
              });

      Supplier<CommandResult> execute =
          new InsertOperation(
                  COMMAND_CONTEXT_NON_VECTOR, List.of(shredDocument1, shredDocument2), false)
              .execute(dataApiRequestInfo, queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution: both called
      assertThat(callCount1.get()).isEqualTo(1);
      assertThat(callCount1.get()).isEqualTo(1);

      // then result has 2 errors
      CommandResult result = execute.get();
      assertThat(result.status()).hasSize(1).containsEntry(CommandStatus.INSERTED_IDS, List.of());
      assertThat(result.errors())
          .hasSize(2)
          .extracting(CommandResult.Error::message)
          .containsExactlyInAnyOrder(
              "Failed to insert document with _id 'doc1': Insert 1 failed",
              "Failed to insert document with _id 'doc2': Insert 2 failed");
    }
  }

  @Nested
  class InsertVector {
    @Test
    public void insertOneVectorSearch() throws Exception {
      String document =
          """
        {
          "_id": "doc1",
          "text": "user1",
          "number" : 10,
          "boolean": true,
          "nullval" : null,
          "array" : ["a", "b"],
          "sub_doc" : {"col": "val"},
          "date_val" : {"$date": 1672531200000 },
          "$vector" : [0.11,0.22,0.33,0.44]
        }
        """;

      JsonNode jsonNode = objectMapper.readTree(document);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      SimpleStatement insertStmt = vectorInsertStatement(shredDocument);
      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertOperation(COMMAND_CONTEXT_VECTOR, shredDocument)
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

    @Test
    public void insertOneVectorEnabledNoVectorData() throws Exception {
      String document =
          """
        {
          "_id": "doc1",
          "text": "user1",
          "number" : 10,
          "boolean": true,
          "nullval" : null,
          "array" : ["a", "b"],
          "sub_doc" : {"col": "val"},
          "date_val" : {"$date": 1672531200000 }
        }
        """;

      JsonNode jsonNode = objectMapper.readTree(document);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);

      SimpleStatement insertStmt = vectorInsertStatement(shredDocument);
      List<Row> rows = Arrays.asList(resultRow(COLUMNS_APPLIED, 0, Boolean.TRUE));
      AsyncResultSet results = new MockAsyncResultSet(COLUMNS_APPLIED, rows, null);
      final AtomicInteger callCount = new AtomicInteger();
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      when(queryExecutor.executeWrite(eq(dataApiRequestInfo), eq(insertStmt)))
          .then(
              invocation -> {
                callCount.incrementAndGet();
                return Uni.createFrom().item(results);
              });

      Supplier<CommandResult> execute =
          new InsertOperation(COMMAND_CONTEXT_VECTOR, shredDocument)
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

    @Test
    public void insertOneVectorDisabledWithVectorData() throws Exception {
      String document =
          """
        {
          "_id": "doc1",
          "text": "user1",
          "number" : 10,
          "boolean": true,
          "nullval" : null,
          "array" : ["a", "b"],
          "sub_doc" : {"col": "val"},
          "date_val" : {"$date": 1672531200000 },
          "$vector" : [0.11,0.22,0.33,0.44]
        }
        """;

      JsonNode jsonNode = objectMapper.readTree(document);
      WritableShreddedDocument shredDocument = shredder.shred(jsonNode);
      InsertOperation operation = new InsertOperation(COMMAND_CONTEXT_NON_VECTOR, shredDocument);
      QueryExecutor queryExecutor = mock(QueryExecutor.class);

      Throwable failure =
          catchThrowable(() -> operation.execute(dataApiRequestInfo, queryExecutor));
      assertThat(failure)
          .isInstanceOf(JsonApiException.class)
          .hasFieldOrPropertyWithValue("errorCode", ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED);
    }
  }

  private MockRow resultRow(ColumnDefinitions columnDefs, int index, Object... values) {
    List<ByteBuffer> buffers = Stream.of(values).map(value -> byteBufferFromAny(value)).toList();
    return new MockRow(columnDefs, index, buffers);
  }

  private SimpleStatement nonVectorInsertStatement(WritableShreddedDocument shredDocument) {
    String insertCql = INSERT_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    return SimpleStatement.newInstance(
        insertCql,
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
  }

  private SimpleStatement vectorInsertStatement(WritableShreddedDocument shredDocument) {
    String insertCql = INSERT_VECTOR_CQL.formatted(KEYSPACE_NAME, COLLECTION_NAME);
    return SimpleStatement.newInstance(
        insertCql,
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
        CQLBindValues.getTimestampMapValues(shredDocument.queryTimestampValues()),
        CQLBindValues.getVectorValue(shredDocument.queryVectorValues()));
  }
}
