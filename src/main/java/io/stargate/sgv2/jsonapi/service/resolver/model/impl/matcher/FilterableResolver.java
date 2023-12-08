package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.quarkus.logging.Log;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocValueHasher;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import jakarta.inject.Inject;
import java.math.BigDecimal;
import java.util.*;

/**
 * Base for resolvers that are {@link Filterable}, there are a number of commands like find,
 * findOne, updateOne that all have a filter.
 *
 * <p>There will be some re-use, and some customisation to work out.
 *
 * <p>T - type of the command we are resolving
 */
public abstract class FilterableResolver<T extends Command & Filterable> {

  private final FilterMatchRules<T> matchRules = new FilterMatchRules<>();

  private static final Object ID_GROUP = new Object();
  private static final Object ID_GROUP_IN = new Object();
  private static final Object ID_GROUP_RANGE = new Object();
  private static final Object DYNAMIC_GROUP_IN = new Object();
  private static final Object DYNAMIC_TEXT_GROUP = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP = new Object();
  private static final Object DYNAMIC_BOOL_GROUP = new Object();
  private static final Object DYNAMIC_NULL_GROUP = new Object();
  private static final Object DYNAMIC_DATE_GROUP = new Object();
  private static final Object EXISTS_GROUP = new Object();
  private static final Object ALL_GROUP = new Object();
  private static final Object SIZE_GROUP = new Object();
  private static final Object ARRAY_EQUALS = new Object();
  private static final Object SUB_DOC_EQUALS = new Object();

  // $ne: 8 captures as following
  private static final Object ID_GROUP_NE = new Object();
  private static final Object DYNAMIC_TEXT_GROUP_NE = new Object();
  private static final Object DYNAMIC_NUMBER_GROUP_NE = new Object();
  private static final Object DYNAMIC_BOOL_GROUP_NE = new Object();
  private static final Object DYNAMIC_NULL_GROUP_NE = new Object();
  private static final Object DYNAMIC_DATE_GROUP_NE = new Object();
  private static final Object ARRAY_EQUALS_NE = new Object();
  private static final Object SUB_DOC_EQUALS_NE = new Object();

  // $nin: 2 captures as following
  private static final Object ID_GROUP_NIN = new Object();
  private static final Object DYNAMIC_GROUP_NIN = new Object();

  @Inject DocumentLimitsConfig docLimits;

  @Inject
  public FilterableResolver() {
    matchRules.addMatchRule(FilterableResolver::findNoFilter, FilterMatcher.MatchStrategy.EMPTY);
    matchRules
        .addMatchRule(FilterableResolver::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID)
        .capture(ID_GROUP_NE)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.NE), JsonType.DOCUMENT_ID);

    matchRules
        .addMatchRule(FilterableResolver::findById, FilterMatcher.MatchStrategy.STRICT)
        .matcher()
        .capture(ID_GROUP_IN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY)
        .capture(ID_GROUP_NIN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.NIN), JsonType.ARRAY);

    matchRules
        .addMatchRule(FilterableResolver::findDynamic, FilterMatcher.MatchStrategy.GREEDY)
        .matcher()
        .capture(ID_GROUP)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.EQ), JsonType.DOCUMENT_ID)
        .capture(ID_GROUP_IN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY)
        .capture(ID_GROUP_RANGE)
        .compareValues(
            "_id",
            EnumSet.of(
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.DOCUMENT_ID)
        .capture(DYNAMIC_GROUP_IN)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.IN), JsonType.ARRAY)
        .capture(DYNAMIC_NUMBER_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.NULL)
        .capture(DYNAMIC_DATE_GROUP)
        .compareValues(
            "*",
            EnumSet.of(
                ValueComparisonOperator.EQ,
                ValueComparisonOperator.GT,
                ValueComparisonOperator.GTE,
                ValueComparisonOperator.LT,
                ValueComparisonOperator.LTE),
            JsonType.DATE)
        .capture(EXISTS_GROUP)
        .compareValues("*", EnumSet.of(ElementComparisonOperator.EXISTS), JsonType.BOOLEAN)
        .capture(ALL_GROUP)
        .compareValues("*", EnumSet.of(ArrayComparisonOperator.ALL), JsonType.ARRAY)
        .capture(SIZE_GROUP)
        .compareValues("*", EnumSet.of(ArrayComparisonOperator.SIZE), JsonType.NUMBER)
        .capture(ARRAY_EQUALS)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.ARRAY)
        .capture(SUB_DOC_EQUALS)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.EQ), JsonType.SUB_DOC)
        // $ne: 8 captures as following
        .capture(ID_GROUP_NE)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.NE), JsonType.DOCUMENT_ID)
        .capture(DYNAMIC_NUMBER_GROUP_NE)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.NE), JsonType.NUMBER)
        .capture(DYNAMIC_TEXT_GROUP_NE)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.NE), JsonType.STRING)
        .capture(DYNAMIC_BOOL_GROUP_NE)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.NE), JsonType.BOOLEAN)
        .capture(DYNAMIC_NULL_GROUP_NE)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.NE), JsonType.NULL)
        .capture(DYNAMIC_DATE_GROUP_NE)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.NE), JsonType.DATE)
        .capture(ARRAY_EQUALS_NE)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.NE), JsonType.ARRAY)
        .capture(SUB_DOC_EQUALS_NE)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.NE), JsonType.SUB_DOC)
        // $nin: 2 captures as following
        .capture(ID_GROUP_NIN)
        .compareValues("_id", EnumSet.of(ValueComparisonOperator.NIN), JsonType.ARRAY)
        .capture(DYNAMIC_GROUP_NIN)
        .compareValues("*", EnumSet.of(ValueComparisonOperator.NIN), JsonType.ARRAY);
  }

  protected LogicalExpression resolve(CommandContext commandContext, T command) {
    Log.error("Filterable Resolver start");
    LogicalExpression filter = matchRules.apply(commandContext, command);
    if (filter.getTotalComparisonExpressionCount() > docLimits.maxFilterObjectProperties()) {
      throw new JsonApiException(
          ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION,
          String.format(
              "%s: filter has %d fields, exceeds maximum allowed %s",
              ErrorCode.FILTER_FIELDS_LIMIT_VIOLATION.getMessage(),
              filter.getTotalComparisonExpressionCount(),
              docLimits.maxFilterObjectProperties()));
    }
    Log.error("Filterable Resolver end " + filter);
    return filter;
  }

  public static List<DBFilterBase> findById(CaptureExpression captureExpression) {
    Log.error("findByID");

    List<DBFilterBase> filters = new ArrayList<>();
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {
      if (captureExpression.marker() == ID_GROUP) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.EQ, (DocumentId) filterOperation.operand().value()));
      }
      if (captureExpression.marker() == ID_GROUP_IN) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.IN,
                (List<DocumentId>) filterOperation.operand().value()));
      }
    }
    return filters;
  }

  public static List<DBFilterBase> findNoFilter(CaptureExpression captureExpression) {
    Log.error("find NO Filter");
    return List.of();
  }

  public static List<DBFilterBase> findDynamic(CaptureExpression captureExpression) {
    Log.error("findByDynamic");

    List<DBFilterBase> filters = new ArrayList<>();
    for (FilterOperation<?> filterOperation : captureExpression.filterOperations()) {

      if (captureExpression.marker() == ID_GROUP) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.EQ, (DocumentId) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == ID_GROUP_IN) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.IN,
                (List<DocumentId>) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == ID_GROUP_RANGE) {
        final DocumentId value = (DocumentId) filterOperation.operand().value();
        if (value.value() instanceof BigDecimal bdv) {
          filters.add(
              new DBFilterBase.NumberFilter(
                  DocumentConstants.Fields.DOC_ID,
                  getDBFilterBaseOperator(filterOperation.operator()),
                  bdv));
        }
        if (value.value() instanceof Map mv) {
          filters.add(
              new DBFilterBase.DateFilter(
                  DocumentConstants.Fields.DOC_ID,
                  getDBFilterBaseOperator(filterOperation.operator()),
                  JsonUtil.createDateFromDocumentId(value)));
        }
      }

      if (captureExpression.marker() == DYNAMIC_GROUP_IN) {
        filters.add(
            new DBFilterBase.InFilter(
                DBFilterBase.InFilter.Operator.IN,
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_TEXT_GROUP) {
        filters.add(
            new DBFilterBase.TextFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.EQ,
                (String) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_BOOL_GROUP) {
        filters.add(
            new DBFilterBase.BoolFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.EQ,
                (Boolean) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NUMBER_GROUP) {
        filters.add(
            new DBFilterBase.NumberFilter(
                captureExpression.path(),
                getDBFilterBaseOperator(filterOperation.operator()),
                (BigDecimal) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NULL_GROUP) {
        filters.add(
            new DBFilterBase.IsNullFilter(
                captureExpression.path(), DBFilterBase.SetFilterBase.Operator.CONTAINS));
      }

      if (captureExpression.marker() == DYNAMIC_DATE_GROUP) {
        filters.add(
            new DBFilterBase.DateFilter(
                captureExpression.path(),
                getDBFilterBaseOperator(filterOperation.operator()),
                (Date) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == EXISTS_GROUP) {
        Boolean bool = (Boolean) filterOperation.operand().value();
        filters.add(new DBFilterBase.ExistsFilter(captureExpression.path(), bool));
      }

      if (captureExpression.marker() == ALL_GROUP) {
        final DocValueHasher docValueHasher = new DocValueHasher();
        List<Object> objects = (List<Object>) filterOperation.operand().value();
        for (Object arrayValue : objects) {
          filters.add(
              new DBFilterBase.AllFilter(docValueHasher, captureExpression.path(), arrayValue));
        }
      }

      if (captureExpression.marker() == SIZE_GROUP) {
        BigDecimal bigDecimal = (BigDecimal) filterOperation.operand().value();
        filters.add(new DBFilterBase.SizeFilter(captureExpression.path(), bigDecimal.intValue()));
      }

      if (captureExpression.marker() == ARRAY_EQUALS) {
        filters.add(
            new DBFilterBase.ArrayEqualsFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value(),
                DBFilterBase.MapFilterBase.Operator.MAP_EQUALS));
      }

      if (captureExpression.marker() == SUB_DOC_EQUALS) {
        filters.add(
            new DBFilterBase.SubDocEqualsFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (Map<String, Object>) filterOperation.operand().value(),
                DBFilterBase.MapFilterBase.Operator.MAP_EQUALS));
      }

      // $ne: 8 captures as following
      if (captureExpression.marker() == ID_GROUP_NE) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.NE, (DocumentId) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_TEXT_GROUP_NE) {
        filters.add(
            new DBFilterBase.TextFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.NE,
                (String) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_BOOL_GROUP_NE) {
        filters.add(
            new DBFilterBase.BoolFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.NE,
                (Boolean) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NUMBER_GROUP_NE) {
        filters.add(
            new DBFilterBase.NumberFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.NE,
                (BigDecimal) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_NULL_GROUP_NE) {
        filters.add(
            new DBFilterBase.IsNullFilter(
                captureExpression.path(), DBFilterBase.SetFilterBase.Operator.NOT_CONTAINS));
      }

      if (captureExpression.marker() == DYNAMIC_DATE_GROUP_NE) {
        filters.add(
            new DBFilterBase.DateFilter(
                captureExpression.path(),
                DBFilterBase.MapFilterBase.Operator.NE,
                (Date) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == ARRAY_EQUALS_NE) {
        filters.add(
            new DBFilterBase.ArrayEqualsFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value(),
                DBFilterBase.MapFilterBase.Operator.MAP_NOT_EQUALS));
      }

      if (captureExpression.marker() == SUB_DOC_EQUALS_NE) {
        filters.add(
            new DBFilterBase.SubDocEqualsFilter(
                new DocValueHasher(),
                captureExpression.path(),
                (Map<String, Object>) filterOperation.operand().value(),
                DBFilterBase.MapFilterBase.Operator.MAP_NOT_EQUALS));
      }
      // $nin: 2 captures as following
      if (captureExpression.marker() == ID_GROUP_NIN) {
        filters.add(
            new DBFilterBase.IDFilter(
                DBFilterBase.IDFilter.Operator.NIN,
                (List<DocumentId>) filterOperation.operand().value()));
      }

      if (captureExpression.marker() == DYNAMIC_GROUP_NIN) {
        filters.add(
            new DBFilterBase.InFilter(
                DBFilterBase.InFilter.Operator.NIN,
                captureExpression.path(),
                (List<Object>) filterOperation.operand().value()));
      }
    }

    return filters;
  }

  private static DBFilterBase.MapFilterBase.Operator getDBFilterBaseOperator(
      FilterOperator filterOperation) {
    switch ((ValueComparisonOperator) filterOperation) {
      case EQ:
        return DBFilterBase.MapFilterBase.Operator.EQ;
      case GT:
        return DBFilterBase.MapFilterBase.Operator.GT;
      case GTE:
        return DBFilterBase.MapFilterBase.Operator.GTE;
      case LT:
        return DBFilterBase.MapFilterBase.Operator.LT;
      case LTE:
        return DBFilterBase.MapFilterBase.Operator.LTE;
      default:
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
            String.format("Unsupported filter operator %s ", filterOperation.getOperator()));
    }
  }
}
