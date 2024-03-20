package io.stargate.sgv2.jsonapi.exception.mappers;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import io.quarkus.security.UnauthorizedException;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.DebugModeConfig;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.OfflineFileWriterInitializer;
import jakarta.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Simple mapper for mapping {@link Throwable}s to {@link CommandResult.Error}, with a default
 * implementation.
 */
public final class ThrowableToErrorMapper {
  private static final BiFunction<Throwable, String, CommandResult.Error> MAPPER_WITH_MESSAGE =
      (throwable, message) -> {
        // if our own exception, shortcut
        if (throwable instanceof JsonApiException jae) {
          return jae.getCommandResultError(message, Response.Status.OK);
        }

        // construct fieldsForMetricsTag, only expose exceptionClass in debugMode
        SmallRyeConfig config;
        if (OfflineFileWriterInitializer.isOffline()) {
          config = new SmallRyeConfigBuilder().withMapping(DebugModeConfig.class).build();
        } else {
          config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
        }
        DebugModeConfig debugModeConfig = config.getConfigMapping(DebugModeConfig.class);
        final boolean debugEnabled = debugModeConfig.enabled();
        Map<String, Object> fields =
            debugEnabled ? Map.of("exceptionClass", throwable.getClass().getSimpleName()) : null;
        final Map<String, Object> fieldsForMetricsTag =
            Map.of("exceptionClass", throwable.getClass().getSimpleName());

        // Authentication Failure -> ErrorCode.UNAUTHORIZED_REQUEST
        if (throwable instanceof UnauthorizedException
            || throwable
                instanceof com.datastax.oss.driver.api.core.servererrors.UnauthorizedException) {
          return ErrorCode.UNAUTHENTICATED_REQUEST
              .toApiException()
              .getCommandResultError(
                  ErrorCode.UNAUTHENTICATED_REQUEST.getMessage(), Response.Status.UNAUTHORIZED);
          // Driver QueryValidationException -> ErrorCode.INVALID_QUERY
        } else if (throwable instanceof QueryValidationException) {
          if (message.contains("vector<float,")) {
            message = "Mismatched vector dimension";
          }
          if (message.contains(
                  "If you want to execute this query despite the performance unpredictability, use ALLOW FILTERING")
              || message.contains("ANN ordering by vector requires the column to be indexed")) {
            return ErrorCode.NO_INDEX_ERROR
                .toApiException()
                .getCommandResultError(ErrorCode.NO_INDEX_ERROR.getMessage(), Response.Status.OK);
          }
          return ErrorCode.INVALID_QUERY
              .toApiException()
              .getCommandResultError(message, Response.Status.OK);
          // Driver Timeout Exception -> ErrorCode.OPERATION_TIMEOUT
        } else if (throwable instanceof DriverTimeoutException
            || throwable instanceof WriteTimeoutException
            || throwable instanceof ReadTimeoutException) {
          return ErrorCode.DRIVER_TIMEOUT
              .toApiException()
              .getCommandResultError(message, Response.Status.INTERNAL_SERVER_ERROR);
          // Driver AllNodesFailedException a composite exception
          // peeling the errors from it
        } else if (throwable instanceof AllNodesFailedException) {
          Map<Node, List<Throwable>> nodewiseErrors =
              ((AllNodesFailedException) throwable).getAllErrors();
          if (!nodewiseErrors.isEmpty()) {
            List<Throwable> errors = nodewiseErrors.values().iterator().next();
            if (errors != null && !errors.isEmpty()) {
              Throwable error =
                  errors.stream()
                      .findAny()
                      .filter(
                          t ->
                              t instanceof AuthenticationException
                                  || t instanceof IllegalArgumentException
                                  || t instanceof NoNodeAvailableException)
                      .orElse(null);
              // connect to oss cassandra throws AuthenticationException for invalid credentials
              // connect to AstraDB throws IllegalArgumentException for invalid token/credentials
              if (error instanceof AuthenticationException
                  || (error instanceof IllegalArgumentException
                      && (error.getMessage().contains("AUTHENTICATION ERROR")
                          || error
                              .getMessage()
                              .contains(
                                  "Provided username token and/or password are incorrect")))) {
                return ErrorCode.UNAUTHENTICATED_REQUEST
                    .toApiException()
                    .getCommandResultError(
                        ErrorCode.UNAUTHENTICATED_REQUEST.getMessage(),
                        Response.Status.UNAUTHORIZED);
                // Driver NoNodeAvailableException -> ErrorCode.NO_NODE_AVAILABLE
              } else if (error instanceof NoNodeAvailableException) {
                return ErrorCode.NO_NODE_AVAILABLE
                    .toApiException()
                    .getCommandResultError(message, Response.Status.INTERNAL_SERVER_ERROR);
              }
            }
          }
          // Driver ClosedConnectionException -> ErrorCode.DRIVER_CLOSED_CONNECTION
        } else if (throwable instanceof ClosedConnectionException) {
          return ErrorCode.DRIVER_CLOSED_CONNECTION
              .toApiException()
              .getCommandResultError(message, Response.Status.INTERNAL_SERVER_ERROR);
          // Unidentified Driver Exceptions, will not map into JsonApiException
        } else if (throwable instanceof DriverException) {
          return new CommandResult.Error(
              message, fieldsForMetricsTag, fields, Response.Status.INTERNAL_SERVER_ERROR);
        }

        return new CommandResult.Error(message, fieldsForMetricsTag, fields, Response.Status.OK);
      };

  private static final Function<Throwable, CommandResult.Error> MAPPER =
      throwable -> {
        String message = throwable.getMessage();
        if (message == null) {
          message = "Unexpected exception occurred.";
        }
        return MAPPER_WITH_MESSAGE.apply(throwable, message);
      };

  private ThrowableToErrorMapper() {}

  public static Function<Throwable, CommandResult.Error> getMapperFunction() {
    return MAPPER;
  }

  public static BiFunction<Throwable, String, CommandResult.Error> getMapperWithMessageFunction() {
    return MAPPER_WITH_MESSAGE;
  }
}
