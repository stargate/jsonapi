package io.stargate.sgv2.jsonapi.service.operation.model;

import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CountOperationPage;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ExpressionBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Operation that returns count of documents based on the filter condition. Written with the
 * assumption that all variables to be indexed.
 */
public record CountOperation(
    CommandContext commandContext, LogicalExpression logicalExpression, int pageSize, int limit)
    implements ReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    SimpleStatement simpleStatement = buildSelectQuery();
    Uni<CountResponse> countResponse = null;
    if (limit == -1)
      countResponse = countDocuments(dataApiRequestInfo, queryExecutor, simpleStatement);
    else countResponse = countDocumentsByKey(dataApiRequestInfo, queryExecutor, simpleStatement);

    return countResponse
        .onItem()
        .transform(
            docs -> {
              if (limit == -1) {
                return new CountOperationPage(docs.count(), false);
              } else {
                boolean moreData = docs.count() > limit();
                return new CountOperationPage(
                    docs.count() > limit() ? docs.count() - 1 : docs.count(), moreData);
              }
            });
  }

  private SimpleStatement buildSelectQuery() {
    final List<Expression<BuiltCondition>> expressions =
        ExpressionBuilder.buildExpressions(logicalExpression, null);
    List<Object> collect = new ArrayList<>();
    if (expressions != null && !expressions.isEmpty() && expressions.get(0) != null) {
      collect = ExpressionBuilder.getExpressionValuesInOrder(expressions.get(0));
    }
    QueryOuterClass.Query query = null;
    if (limit == -1) {
      query =
          new QueryBuilder()
              .select()
              .count()
              .as("count")
              .from(commandContext.namespace(), commandContext.collection())
              .where(expressions.get(0))
              .build();
    } else {
      query =
          new QueryBuilder()
              .select()
              .column("key")
              .from(commandContext.namespace(), commandContext.collection())
              .where(expressions.get(0))
              .limit(limit + 1)
              .build();
    }

    final SimpleStatement simpleStatement = SimpleStatement.newInstance(query.getCql());
    simpleStatement.setPageSize(pageSize());
    return simpleStatement.setPositionalValues(collect);
  }
}
