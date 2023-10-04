package io.stargate.sgv2.jsonapi.service.resolver.model.impl.matcher;

import io.quarkus.logging.Log;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.function.Function;

/**
 * This class matches the filter clauses against the filter match rules defined. The match rules
 * will be defined in order of preference, so first best match will be used for query processing
 *
 * @param <T> should be a {@link Command} type, which also implements {@link Filterable}
 */
public class FilterMatcher<T extends Command & Filterable> {

  private List<Capture> captures = new ArrayList<>();

  public enum MatchStrategy {
    EMPTY,
    STRICT, // every capture must match once and only once, every expression must match
    GREEDY, // capture groups can match zero or more times, every expression must match
  }

  private final MatchStrategy strategy;

  private final Function<CaptureExpression, List<DBFilterBase>> resolveFunction;

  FilterMatcher(
      MatchStrategy strategy, Function<CaptureExpression, List<DBFilterBase>> resolveFunction) {
    this.strategy = strategy;
    this.resolveFunction = resolveFunction;
  }

  public Optional<LogicalExpression> apply(T command) {

    FilterClause filter = command.filterClause();
    Log.error("matcher apply " + filter.logicalExpression());

    //    CaptureGroups returnedCaptureGroups = new CaptureGroups(command);
    if (strategy == MatchStrategy.EMPTY) {
      if (filter == null
          || (filter.logicalExpression().logicalExpressions.isEmpty()
              && filter.logicalExpression().comparisonExpressions.isEmpty())) {
        return Optional.of(filter.logicalExpression()); // TODO
      } else {
        return Optional.empty();
      }
    }
    if (filter == null) {
      return Optional.empty();
    }

    // 在strict的模式下, baseCaptures个会被 iterator 删除，，用完拉到
    List<Capture> unmatchedCaptures = new ArrayList<>(captures);
    final MatchStrategyCounter matchStrategyCounter =
        new MatchStrategyCounter(
            unmatchedCaptures.size(), filter.logicalExpression().totalComparisonExpressionCount);
    captureRecursive(filter.logicalExpression(), unmatchedCaptures, matchStrategyCounter);
    Log.error("after captureRecursive  " + filter.logicalExpression());

    // these strategies should be abstracted if we have another one, only 2 for now.
    switch (strategy) {
      case STRICT:
        if (matchStrategyCounter.unmatchedCaptureCount == 0
            && matchStrategyCounter.unmatchedComparisonExpressionCount == 0) {
          // everything group and expression matched
          Log.error("matcher strict " + filter.logicalExpression());
          return Optional.of(filter.logicalExpression());
        }
        break;
      case GREEDY:
        if (matchStrategyCounter.unmatchedComparisonExpressionCount == 0) {
          // everything expression matched, some captures may not match
          Log.error("matcher greedy");
          return Optional.of(filter.logicalExpression());
        }
        break;
    }
    return Optional.empty();
  }

  public void captureRecursive(
      LogicalExpression expression,
      List<Capture> unmatchedCaptures,
      MatchStrategyCounter matchStrategyCounter) {
    for (LogicalExpression logicalExpression : expression.logicalExpressions) {
      captureRecursive(logicalExpression, unmatchedCaptures, matchStrategyCounter);
    }
    ListIterator<ComparisonExpression> expressionIterator =
        expression.comparisonExpressions.listIterator();
    while (expressionIterator.hasNext()) {
      ComparisonExpression comparisonExpression = expressionIterator.next();
      ListIterator<Capture> captureIter = unmatchedCaptures.listIterator();
      while (captureIter.hasNext()) {
        Capture capture = captureIter.next();
        List<FilterOperation<?>> matched = capture.match(comparisonExpression); // TODO 会有多个？？？？
        if (!matched.isEmpty()) { // 说明这一轮的comparisonExpression被match了 那么我们需要把matched放起来
          //          comparisonExpression
          //              .getGroup(capture.marker)
          //              .withCapture(comparisonExpression.getPath(), matched);
          // TODO  index of matched 会有多个？？？
          comparisonExpression.setDBFilters(
              resolveFunction.apply(
                  new CaptureExpression(capture.marker, matched, comparisonExpression.getPath())));
          //          comparisonExpression.setDBFilters(
          //              resolveFunction.apply(
          //                  capture.marker,
          //                  new CaptureExpression(
          //                      comparisonExpression.getPath(),
          //                      matched.get(0).operator(),
          //                      matched.get(0).operand().value())));

          switch (strategy) {
            case STRICT:
              captureIter.remove();
              matchStrategyCounter.decreaseUnmatchedCaptureCount();
              ;
              //              expressionIterator.remove();
              matchStrategyCounter.decreaseUnmatchedComparisonExpressionCount();
              break;
            case GREEDY:
              //              expressionIterator.remove();
              matchStrategyCounter.decreaseUnmatchedComparisonExpressionCount();
              break;
          }
          break;
        }
      }
    }
  }

  /**
   * Start of the fluent API, create a Capture then add the matching
   *
   * <p>See {@link FilterMatchRules #addMatchRule(BiFunction, MatchStrategy)}}
   *
   * @param marker
   * @return
   */
  public Capture capture(Object marker) {
    final Capture newCapture = new Capture(marker);
    captures.add(newCapture);
    return newCapture;
  }

  /**
   * Capture provides a fluent API to build the matchers to apply to the filter.
   *
   * <p>**NOTE:** Is a non static class, it is bound to an instance of FilterMatcher to provide the
   * fluent API.
   */
  public final class Capture {

    private Object marker;
    private String matchPath;
    private EnumSet operators;
    private JsonType type;

    protected Capture(Object marker) {
      this.marker = marker;
    }

    public List<FilterOperation<?>> match(ComparisonExpression t) {
      return t.match(matchPath, operators, type);
    }

    /**
     * The path is compared using an operator against a value of a type
     *
     * <p>e.g. <code>
     *  .compare("*", ValueComparisonOperator.GT, JsonType.NUMBER);
     * </code>
     *
     * @param path
     * @param type
     * @return
     */
    public FilterMatcher<T> compareValues(
        String path, EnumSet<? extends FilterOperator> operators, JsonType type) {
      this.matchPath = path;
      this.operators = operators;
      this.type = type;
      return FilterMatcher.this;
    }
  }

  public final class MatchStrategyCounter {

    private int unmatchedCaptureCount;
    private int unmatchedComparisonExpressionCount;

    public MatchStrategyCounter(int unmatchedCaptureCount, int unmatchedComparisonExpressionCount) {
      this.unmatchedCaptureCount = unmatchedCaptureCount;
      this.unmatchedComparisonExpressionCount = unmatchedComparisonExpressionCount;
    }

    public void decreaseUnmatchedCaptureCount() {
      unmatchedCaptureCount--;
    }

    public void decreaseUnmatchedComparisonExpressionCount() {
      unmatchedComparisonExpressionCount--;
    }
  }
}
