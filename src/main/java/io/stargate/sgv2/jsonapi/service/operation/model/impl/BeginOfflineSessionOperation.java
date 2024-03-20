package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.FileWriterSession;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.Map;
import java.util.function.Supplier;

public record BeginOfflineSessionOperation(
    CommandContext ctx,
    String sessionId,
    FileWriterParams fileWriterParams,
    Shredder shredder,
    ObjectMapper objectMapper)
    implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    try {
      dataApiRequestInfo.setTenantId(sessionId);
      CQLSessionCache.SessionCacheKey sessionCacheKey =
          new CQLSessionCache.SessionCacheKey(dataApiRequestInfo.getTenantId().orElseThrow(), null);
      FileWriterSession fileWriterSession =
          new FileWriterSession(
              queryExecutor.getCqlSessionCache(), sessionCacheKey, sessionId, fileWriterParams);
      queryExecutor.getCqlSessionCache().putSession(sessionCacheKey, fileWriterSession);
      CommandResult commandResult =
          new CommandResult(Map.of(CommandStatus.OFFLINE_WRITER_SESSION_ID, sessionId));
      return Uni.createFrom().item(() -> () -> commandResult);
    } catch (Exception e) {
      JsonApiException jsonApiException =
          new JsonApiException(
              ErrorCode.UNABLE_TO_CREATE_OFFLINE_WRITER_SESSION,
              ErrorCode.UNABLE_TO_CREATE_OFFLINE_WRITER_SESSION.getMessage() + sessionId);
      jsonApiException.initCause(e);
      return Uni.createFrom().failure(jsonApiException);
    }
  }
}
