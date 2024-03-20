package io.stargate.sgv2.jsonapi.service.processor;

import static io.stargate.sgv2.api.common.config.constants.LoggingConstants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.config.MetricsConfig;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.Filterable;
import io.stargate.sgv2.jsonapi.api.model.command.Sortable;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortExpression;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonApiMetricsConfig;
import io.stargate.sgv2.jsonapi.config.CommandLevelLoggingConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

@ApplicationScoped
public class MeteredCommandProcessor {

  private static final Logger logger = LoggerFactory.getLogger(MeteredCommandProcessor.class);

  private static final String UNKNOWN_VALUE = "unknown";

  private static final String NA = "NA";

  private final CommandProcessor commandProcessor;

  private final MeterRegistry meterRegistry;

  private final DataApiRequestInfo dataApiRequestInfo;

  private final JsonApiMetricsConfig jsonApiMetricsConfig;

  private final MetricsConfig.TenantRequestCounterConfig tenantConfig;

  /** The tag for error being true, created only once. */
  private final Tag errorTrue;

  /** The tag for error being false, created only once. */
  private final Tag errorFalse;

  /** The tag for tenant being unknown, created only once. */
  private final Tag tenantUnknown;

  private final Tag defaultErrorCode;

  private final Tag defaultErrorClass;
  private final CommandLevelLoggingConfig commandLevelLoggingConfig;

  @Inject
  public MeteredCommandProcessor(
      CommandProcessor commandProcessor,
      MeterRegistry meterRegistry,
      DataApiRequestInfo dataApiRequestInfo,
      JsonApiMetricsConfig jsonApiMetricsConfig,
      MetricsConfig metricsConfig,
      CommandLevelLoggingConfig commandLevelLoggingConfig) {
    this.commandProcessor = commandProcessor;
    this.meterRegistry = meterRegistry;
    this.jsonApiMetricsConfig = jsonApiMetricsConfig;
    tenantConfig = metricsConfig.tenantRequestCounter();
    this.dataApiRequestInfo = dataApiRequestInfo;
    errorTrue = Tag.of(tenantConfig.errorTag(), "true");
    errorFalse = Tag.of(tenantConfig.errorTag(), "false");
    tenantUnknown = Tag.of(tenantConfig.tenantTag(), UNKNOWN_VALUE);
    defaultErrorCode = Tag.of(jsonApiMetricsConfig.errorCode(), NA);
    defaultErrorClass = Tag.of(jsonApiMetricsConfig.errorClass(), NA);
    this.commandLevelLoggingConfig = commandLevelLoggingConfig;
  }

  /**
   * Processes a single command in a given command context.
   *
   * @param commandContext {@link CommandContext}
   * @param command {@link Command}
   * @param <T> Type of the command.
   * @return Uni emitting the result of the command execution.
   */
  public <T extends Command> Uni<CommandResult> processCommand(
      DataApiRequestInfo dataApiRequestInfo, CommandContext commandContext, T command) {
    Timer.Sample sample = Timer.start(meterRegistry);
    MDC.put("tenantId", dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE));
    // start by resolving the command, get resolver
    return commandProcessor
        .processCommand(dataApiRequestInfo, commandContext, command)
        .onItem()
        .invoke(
            result -> {
              Tags tags = getCustomTags(commandContext, command, result);
              // add metrics
              sample.stop(meterRegistry.timer(jsonApiMetricsConfig.metricsName(), tags));

              if (isCommandLevelLoggingEnabled(result, false)) {
                logger.info(buildCommandLog(commandContext, command, result));
              }
            })
        .onFailure()
        .invoke(
            throwable -> {
              if (isCommandLevelLoggingEnabled(null, true)) {
                logger.error(buildCommandLog(commandContext, command, null), throwable);
              }
            });
  }

  /**
   * Builds the command level log in string format.
   *
   * @param commandContext Command context
   * @param command Command
   * @param result Command result
   * @return Command log in string format
   */
  private String buildCommandLog(
      CommandContext commandContext, Command command, CommandResult result) {
    CommandLog commandLog =
        new CommandLog(
            command.getClass().getSimpleName(),
            dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE),
            commandContext.namespace(),
            commandContext.collection(),
            getIncomingDocumentsCount(command),
            getOutgoingDocumentsCount(result),
            result != null ? result.errors() : Collections.emptyList());
    return new ObjectMapper().valueToTree(commandLog).toString();
  }

  /**
   * Get outgoing documents count in a command result.
   *
   * @param result
   * @return
   */
  private String getOutgoingDocumentsCount(CommandResult result) {
    if (result == null) {
      return "NA";
    }
    if (result.data() instanceof CommandResult.MultiResponseData
        || result.data() instanceof CommandResult.SingleResponseData) {
      return String.valueOf(result.data().getResponseDocuments().size());
    }
    return "NA";
  }

  /**
   * Get incoming documents count in a command.
   *
   * @param command
   * @return
   */
  private String getIncomingDocumentsCount(Command command) {
    if (command instanceof InsertManyCommand) {
      return String.valueOf(((InsertManyCommand) command).documents().size());
    } else if (command instanceof InsertOneCommand) {
      return String.valueOf(((InsertOneCommand) command).document() != null ? 1 : 0);
    }
    return "NA";
  }

  /**
   * @param commandResult - command result
   * @param isFailure - Is from the failure flow
   * @return true if command level logging is allowed, false otherwise
   */
  private boolean isCommandLevelLoggingEnabled(CommandResult commandResult, boolean isFailure) {
    if (!commandLevelLoggingConfig.enabled()) {
      return false;
    }
    Set<String> allowedTenants =
        commandLevelLoggingConfig.enabledTenants().orElse(Collections.singleton(ALL_TENANTS));
    if (!allowedTenants.contains(ALL_TENANTS)
        && !allowedTenants.contains(dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE))) {
      return false;
    }
    if (!isFailure
        && commandLevelLoggingConfig.onlyResultsWithErrors()
        && (commandResult == null || commandResult.errors().isEmpty())) {
      return false;
    }
    // return true in all other cases
    return true;
  }

  /**
   * Generate custom tags based on the command and result.
   *
   * @param command - request command
   * @param result - response command result
   * @return
   */
  private Tags getCustomTags(CommandContext commandContext, Command command, CommandResult result) {
    Tag commandTag = Tag.of(jsonApiMetricsConfig.command(), command.getClass().getSimpleName());
    String tenant = dataApiRequestInfo.getTenantId().orElse(UNKNOWN_VALUE);
    Tag tenantTag = Tag.of(tenantConfig.tenantTag(), tenant);
    Tag errorTag = errorFalse;
    Tag errorClassTag = defaultErrorClass;
    Tag errorCodeTag = defaultErrorCode;
    // if error is present, add error tags else use defaults
    if (null != result.errors() && !result.errors().isEmpty()) {
      errorTag = errorTrue;
      String errorClass =
          (String)
              result
                  .errors()
                  .get(0)
                  .fieldsForMetricsTag()
                  .getOrDefault("exceptionClass", UNKNOWN_VALUE);
      errorClassTag = Tag.of(jsonApiMetricsConfig.errorClass(), errorClass);
      String errorCode =
          (String)
              result.errors().get(0).fieldsForMetricsTag().getOrDefault("errorCode", UNKNOWN_VALUE);
      errorCodeTag = Tag.of(jsonApiMetricsConfig.errorCode(), errorCode);
    }
    Tag vectorEnabled =
        commandContext.isVectorEnabled()
            ? Tag.of(jsonApiMetricsConfig.vectorEnabled(), "true")
            : Tag.of(jsonApiMetricsConfig.vectorEnabled(), "false");
    JsonApiMetricsConfig.SortType sortType = getVectorTypeTag(command);
    Tag sortTypeTag = Tag.of(jsonApiMetricsConfig.sortType(), sortType.name());
    Tags tags =
        Tags.of(
            commandTag,
            tenantTag,
            errorTag,
            errorClassTag,
            errorCodeTag,
            vectorEnabled,
            sortTypeTag);
    return tags;
  }

  private JsonApiMetricsConfig.SortType getVectorTypeTag(Command command) {
    int filterCount = 0;
    if (command instanceof Filterable fc && fc.filterClause() != null) {
      filterCount = fc.filterClause().logicalExpression().getTotalComparisonExpressionCount();
    }
    if (command instanceof Sortable sc
        && sc.sortClause() != null
        && !sc.sortClause().sortExpressions().isEmpty()) {
      if (sc.sortClause() != null) {
        List<SortExpression> sortClause = sc.sortClause().sortExpressions();
        if (sortClause.size() == 1
            && (DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD.equals(sortClause.get(0).path())
                || DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD.equals(
                    sortClause.get(0).path()))) {
          if (filterCount == 0) {
            return JsonApiMetricsConfig.SortType.SIMILARITY_SORT;
          } else {
            return JsonApiMetricsConfig.SortType.SIMILARITY_SORT_WITH_FILTERS;
          }
        } else {
          return JsonApiMetricsConfig.SortType.SORT_BY_FIELD;
        }
      }
    }
    return JsonApiMetricsConfig.SortType.NONE;
  }

  /** Enable histogram buckets for a specific timer */
  private static final String HISTOGRAM_METRICS_NAME = "http.server.requests";

  @Produces
  @Singleton
  public MeterFilter enableHistogram() {
    return new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(
          Meter.Id id, DistributionStatisticConfig config) {
        if (id.getName().startsWith(jsonApiMetricsConfig.metricsName())
            || id.getName().startsWith(HISTOGRAM_METRICS_NAME)
            || id.getName().startsWith(jsonApiMetricsConfig.jsonBytesWritten())
            || id.getName().startsWith(jsonApiMetricsConfig.jsonBytesRead())
            || id.getName().startsWith(jsonApiMetricsConfig.jsonDocsWritten())
            || id.getName().startsWith(jsonApiMetricsConfig.jsonDocsRead())) {
          return DistributionStatisticConfig.builder()
              .percentiles(0.5, 0.90, 0.95, 0.99) // median and 95th percentile, not aggregable
              .percentilesHistogram(true) // histogram buckets (e.g. prometheus histogram_quantile)
              .build()
              .merge(config);
        }
        return config;
      }
    };
  }
}
