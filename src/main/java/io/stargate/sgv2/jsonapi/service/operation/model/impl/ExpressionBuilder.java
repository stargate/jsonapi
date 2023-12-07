package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Variable;
import io.quarkus.logging.Log;
import io.stargate.sgv2.api.common.cql.ExpressionUtils;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ExpressionBuilder {

  public record ExpressionBuiltResult(
      List<Expression<BuiltCondition>> expressions, Boolean allowFiltering) {}

  public static ExpressionBuiltResult buildExpressions(
      LogicalExpression logicalExpression, DBFilterBase.IDFilter additionalIdFilter) {
    // an empty filter should find everything
    if (logicalExpression.isEmpty() && additionalIdFilter == null) {
      return new ExpressionBuiltResult(Collections.singletonList(null), false);
    }
    // after validate in FilterClauseDeserializer,
    // partition key column key will not be nested under OR operator
    // so we can collect all id_conditions, then do a combination to generate separate queries
    List<DBFilterBase.IDFilter> idFilters = new ArrayList<>();
    // expressionWithoutId must be a And(if not null)
    // since we have outer implicit and in the filter
    Expression<BuiltCondition> expressionWithoutId =
        buildExpressionRecursive(logicalExpression, additionalIdFilter, idFilters);
    Log.error("expressionWithoutId " + expressionWithoutId);
    ExpressionBuiltResult expressionBuiltResult =
        buildExpressionWithId(additionalIdFilter, expressionWithoutId, idFilters);

    Log.error("pairOfExpressionsAndAllowFiltering !! " + expressionBuiltResult);
    return expressionBuiltResult;
  }

  private static ExpressionBuiltResult buildExpressionWithId(
      DBFilterBase.IDFilter additionalIdFilter,
      Expression<BuiltCondition> expressionWithoutId,
      List<DBFilterBase.IDFilter> idFilters) {
    List<Expression<BuiltCondition>> expressionsWithId = new ArrayList<>();
    boolean allow_filtering;
    if (idFilters.size() > 1) {
      throw new JsonApiException(
          ErrorCode.FILTER_MULTIPLE_ID_FILTER, ErrorCode.FILTER_MULTIPLE_ID_FILTER.getMessage());
    }
    if (idFilters.isEmpty()
        && additionalIdFilter == null) { // no idFilters in filter clause and no additionalIdFilter
      if (expressionWithoutId == null) {
        //no valid non_id filters (eg. "name":{"$nin" : []} ) and no id filter
        return new ExpressionBuiltResult(Collections.singletonList(null), false); // should find everything
      } else {
        return new ExpressionBuiltResult(List.of(expressionWithoutId), false);
      }
    }

    // have an idFilter
    DBFilterBase.IDFilter idFilter =
        additionalIdFilter != null ? additionalIdFilter : idFilters.get(0);
    allow_filtering = idFilter.allowFiltering();
    boolean finalAllow_filtering = allow_filtering;

    // _id: {$in: []} should find nothing in the entire query
    // since _id can not work with $or, entire $and should find nothing
    if (idFilter.operator == DBFilterBase.IDFilter.Operator.IN && idFilter.getAll().isEmpty()) {
      return new ExpressionBuiltResult(null, false); // should find nothing
    }

    // _id: {$nin: []} should return true
    // if expressionWithoutId is null, find everything ~
    // if expressionWithoutId is not null, apply expressionWithoutId to filter
    if (idFilter.operator == DBFilterBase.IDFilter.Operator.NIN && idFilter.getAll().isEmpty()) {
      return new ExpressionBuiltResult(
          expressionWithoutId == null
              ? Collections.singletonList(null)
              : List.of(expressionWithoutId),
          false);
    }

    // from here, idFilter is not empty

    // $nin will not split n queries, need allow filtering
    if (idFilter.operator.equals(DBFilterBase.IDFilter.Operator.NIN)) {
      List<Variable<BuiltCondition>> inConditionsVariables =
          idFilter.getAll().stream().map(Variable::of).toList();
      Expression<BuiltCondition> newExpression =
          expressionWithoutId == null
              ? ExpressionUtils.andOf(inConditionsVariables)
              : ExpressionUtils.andOf(
                  ExpressionUtils.andOf(inConditionsVariables), expressionWithoutId);
      return new ExpressionBuiltResult(List.of(newExpression), finalAllow_filtering);
    }

    // $in, idFilter's operator is IN or EQ, for both, split into n query logic
    List<BuiltCondition> inSplit =
        idFilters.isEmpty() ? new ArrayList<>() : idFilters.get(0).getAll();
    if (additionalIdFilter != null) {
      inSplit = additionalIdFilter.getAll(); // override the existed id filter
    }
    return inSplit.stream()
        .map(
            idCondition -> {
              Expression<BuiltCondition> newExpression =
                  expressionWithoutId == null
                      ? Variable.of(idCondition)
                      : ExpressionUtils.andOf(Variable.of(idCondition), expressionWithoutId);
              return newExpression;
            })
        .collect(
            Collectors.collectingAndThen(
                Collectors.toList(),
                expressions -> new ExpressionBuiltResult(expressions, finalAllow_filtering)));
  }

  private static Expression<BuiltCondition> buildExpressionRecursive(
      LogicalExpression logicalExpression,
      DBFilterBase.IDFilter additionalIdFilter,
      List<DBFilterBase.IDFilter> idConditionExpressions) {
    List<Expression<BuiltCondition>> conditionExpressions = new ArrayList<>();
    // first for loop, is to iterate all subLogicalExpression
    // each iteration goes into another recursive build
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      final Expression<BuiltCondition> subExpressionCondition =
          buildExpressionRecursive(
              subLogicalExpression, additionalIdFilter, idConditionExpressions);
      if (subExpressionCondition == null) {
        continue;
      }
      conditionExpressions.add(subExpressionCondition);
    }

    // if seeing $in, set hasInFilterThisLevel as true
    boolean hasInFilterThisLevel = false;
    // if seeing $nin, set hasNinFilterThisLevel as true
    boolean hasNinFilterThisLevel = false;
    boolean inFilterThisLevelWithEmptyArray = true;
    boolean ninFilterThisLevelWithEmptyArray = true;

    // second for loop, is to iterate all subComparisonExpression
    for (ComparisonExpression comparisonExpression : logicalExpression.comparisonExpressions) {
      for (DBFilterBase dbFilter : comparisonExpression.getDbFilters()) {
        if (dbFilter instanceof DBFilterBase.InFilter inFilter) {
          if (inFilter.operator.equals(DBFilterBase.InFilter.Operator.IN)) {
            hasInFilterThisLevel = true;
          } else if (inFilter.operator.equals(DBFilterBase.InFilter.Operator.NIN)) {
            hasNinFilterThisLevel = true;
          }
          List<BuiltCondition> inFilterConditions = inFilter.getAll();
          if (!inFilterConditions.isEmpty()) {
            // store information of an empty array happens with $in or $nin
            if (inFilter.operator.equals(DBFilterBase.InFilter.Operator.IN)) {
              inFilterThisLevelWithEmptyArray = false;
            } else if (inFilter.operator.equals(DBFilterBase.InFilter.Operator.NIN)) {
              ninFilterThisLevelWithEmptyArray = false;
            }
            List<Variable<BuiltCondition>> inConditionsVariables =
                inFilterConditions.stream().map(Variable::of).toList();
            // $in:["A","B"] -> array_contains contains A or array_contains contains B
            // $nin:["A","B"] -> array_contains not contains A and array_contains not
            // contains B
            conditionExpressions.add(
                inFilter.operator.equals(DBFilterBase.InFilter.Operator.IN)
                    ? ExpressionUtils.orOf(inConditionsVariables)
                    : ExpressionUtils.andOf(inConditionsVariables));
          }
        } else if (dbFilter instanceof DBFilterBase.IDFilter idFilter) {
          if (additionalIdFilter == null) {
            idConditionExpressions.add(idFilter);
          }
        } else {
          conditionExpressions.add(Variable.of(dbFilter.get()));
        }
      }
    }

    // when having an empty array $nin, if $nin occurs within an $or logic, entire $or should match
    // everything
    if (hasNinFilterThisLevel
        && ninFilterThisLevelWithEmptyArray
        && logicalExpression.getLogicalRelation().equals(LogicalExpression.LogicalOperator.OR)) {
      Log.error("qweqweqeqw");
      // TODO: find a better CQL TRUE placeholder
      conditionExpressions.clear();
      conditionExpressions.add(
          Variable.of(
              new DBFilterBase.IsNullFilter(
                      "something user never use", DBFilterBase.SetFilterBase.Operator.NOT_CONTAINS)
                  .get()));
      return ExpressionUtils.buildExpression(
          conditionExpressions, logicalExpression.getLogicalRelation().getOperator());
    }

    // when having an empty array $in, if $in occurs within an $and logic, entire $and should match
    // nothing
    if (hasInFilterThisLevel
        && inFilterThisLevelWithEmptyArray
        && logicalExpression.getLogicalRelation().equals(LogicalExpression.LogicalOperator.AND)) {
      Log.error("etere122112 " + conditionExpressions);
      // TODO: find a better CQL FALSE placeholder
      conditionExpressions.clear();
      conditionExpressions.add(
              Variable.of(
                      new DBFilterBase.IsNullFilter(
                              "something user never use", DBFilterBase.SetFilterBase.Operator.CONTAINS)
                              .get()));
      return ExpressionUtils.buildExpression(
              conditionExpressions, logicalExpression.getLogicalRelation().getOperator());
//      return null;
    }

    Log.error("etere " + conditionExpressions);
    // current logicalExpression is empty (implies sub-logicalExpression and
    // sub-comparisonExpression are all empty)
    if (conditionExpressions.isEmpty()) {
      return null;
    }

    return ExpressionUtils.buildExpression(
        conditionExpressions, logicalExpression.getLogicalRelation().getOperator());
  }

  /**
   * Get all positional cql values from express recursively. Result order is in consistent of the
   * expression structure
   */
  public static List<Object> getExpressionValuesInOrder(Expression<BuiltCondition> expression) {
    List<Object> values = new ArrayList<>();
    if (expression != null) {
      populateValuesRecursive(values, expression);
    }
    return values;
  }

  private static void populateValuesRecursive(
      List<Object> values, Expression<BuiltCondition> outerExpression) {
    if (outerExpression.getExprType().equals("variable")) {
      Variable<BuiltCondition> var = (Variable<BuiltCondition>) outerExpression;
      JsonTerm term = ((JsonTerm) var.getValue().value());
      if (term.getKey() != null) {
        values.add(term.getKey());
      }
      values.add(term.getValue());
      return;
    }
    if (outerExpression.getExprType().equals("and") || outerExpression.getExprType().equals("or")) {
      List<Expression<BuiltCondition>> innerExpressions = outerExpression.getChildren();
      for (Expression<BuiltCondition> innerExpression : innerExpressions) {
        populateValuesRecursive(values, innerExpression);
      }
    }
  }
}
