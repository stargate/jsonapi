package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.bridge.grpc.TypeSpecs;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.common.bridge.AbstractValidatingStargateBridgeTest;
import io.stargate.sgv2.common.bridge.ValidatingStargateBridge;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.CountOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.testutil.MockAsyncResultSet;
import io.stargate.sgv2.jsonapi.service.testutil.MockRow;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class CountOperationTest extends AbstractValidatingStargateBridgeTest {
  private static final String KEYSPACE_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final String COLLECTION_NAME = RandomStringUtils.randomAlphanumeric(16);
  private static final CommandContext CONTEXT = new CommandContext(KEYSPACE_NAME, COLLECTION_NAME);

  @Inject QueryExecutor queryExecutor;

  @Nested
  class Execute {

    @Test
    public void countWithNoFilter() {
      String collectionReadCql =
          "SELECT COUNT(1) AS count FROM \"%s\".\"%s\"".formatted(KEYSPACE_NAME, COLLECTION_NAME);
      SimpleStatement stmt = SimpleStatement.newInstance(collectionReadCql);
      QueryExecutor queryExecutorMock = mock(QueryExecutor.class);
      ColumnDefinitions columnDefs =
          buildColumnDefs(Arrays.asList("count"), Arrays.asList(ProtocolConstants.DataType.INT));
      List<Row> rows = Arrays.asList(new MockRow(columnDefs, 0));
      AsyncResultSet mockResults = new MockAsyncResultSet(columnDefs, rows, null);
      when(queryExecutorMock.executeRead(eq(stmt), any(), anyInt()))
          .thenReturn(Uni.createFrom().item(mockResults));
      /*
      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("count")
                          .setType(TypeSpecs.INT)
                          .build()))
              .returning(List.of(List.of(Values.of(5))));
       */

      LogicalExpression implicitAnd = LogicalExpression.and();
      CountOperation countOperation = new CountOperation(CONTEXT, implicitAnd);
      Supplier<CommandResult> execute =
          countOperation
              .execute(queryExecutorMock)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      //      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isNotNull();
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isEqualTo(5);
              });
    }

    ColumnDefinitions buildColumnDefs(List<String> columnNames, List<Integer> typeCodes) {
      return buildColumnDefs(KEYSPACE_NAME, COLLECTION_NAME, columnNames, typeCodes);
    }

    ColumnDefinitions buildColumnDefs(
        String ks, String tableName, List<String> columnNames, List<Integer> typeCodes) {
      List<ColumnDefinition> columnDefs = new ArrayList<>();
      for (int ix = 0, end = columnNames.size(); ix < end; ++ix) {
        columnDefs.add(
            new DefaultColumnDefinition(
                new ColumnSpec(
                    ks,
                    tableName,
                    columnNames.get(ix),
                    ix,
                    RawType.PRIMITIVES.get(typeCodes.get(ix))),
                AttachmentPoint.NONE));
      }
      return DefaultColumnDefinitions.valueOf(columnDefs);
    }

    @Disabled
    @Test
    public void countWithDynamic() {
      String collectionReadCql =
          "SELECT COUNT(1) AS count FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user1").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("count")
                          .setType(TypeSpecs.INT)
                          .build()))
              .returning(List.of(List.of(Values.of(2))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      implicitAnd
          .comparisonExpressions
          .get(0)
          .setDBFilters(
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user1")));
      CountOperation countOperation = new CountOperation(CONTEXT, implicitAnd);
      Supplier<CommandResult> execute =
          countOperation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isNotNull();
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isEqualTo(2);
              });
    }

    @Disabled
    @Test
    public void countWithDynamicNoMatch() {
      String collectionReadCql =
          "SELECT COUNT(1) AS count FROM \"%s\".\"%s\" WHERE array_contains CONTAINS ?"
              .formatted(KEYSPACE_NAME, COLLECTION_NAME);

      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(
                  collectionReadCql,
                  Values.of("username " + new DocValueHasher().getHash("user_all").hash()))
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("count")
                          .setType(TypeSpecs.INT)
                          .build()))
              .returning(List.of(List.of(Values.of(0))));

      LogicalExpression implicitAnd = LogicalExpression.and();
      implicitAnd.comparisonExpressions.add(new ComparisonExpression(null, null, null));
      implicitAnd
          .comparisonExpressions
          .get(0)
          .setDBFilters(
              List.of(
                  new DBFilterBase.TextFilter(
                      "username", DBFilterBase.MapFilterBase.Operator.EQ, "user_all")));

      CountOperation countOperation = new CountOperation(CONTEXT, implicitAnd);
      Supplier<CommandResult> execute =
          countOperation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      CommandResult result = execute.get();
      assertThat(result)
          .satisfies(
              commandResult -> {
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isNotNull();
                assertThat(result.status().get(CommandStatus.COUNTED_DOCUMENT)).isEqualTo(0);
              });
    }

    @Disabled
    @Test
    public void error() {
      // failures are propagated down
      RuntimeException failure = new RuntimeException("Ivan fails the test.");

      String collectionReadCql =
          "SELECT COUNT(1) AS count FROM \"%s\".\"%s\"".formatted(KEYSPACE_NAME, COLLECTION_NAME);

      ValidatingStargateBridge.QueryAssert candidatesAssert =
          withQuery(collectionReadCql)
              .withPageSize(1)
              .withColumnSpec(
                  List.of(
                      QueryOuterClass.ColumnSpec.newBuilder()
                          .setName("count")
                          .setType(TypeSpecs.INT)
                          .build()))
              .returningFailure(failure);

      LogicalExpression implicitAnd = LogicalExpression.and();
      CountOperation countOperation = new CountOperation(CONTEXT, implicitAnd);
      Throwable result =
          countOperation
              .execute(queryExecutor)
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      // assert query execution
      candidatesAssert.assertExecuteCount().isOne();

      // then result
      assertThat(result).isEqualTo(failure);
    }
  }
}
