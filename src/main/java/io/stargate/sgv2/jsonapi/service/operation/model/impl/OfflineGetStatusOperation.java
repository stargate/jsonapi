package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.OfflineGetStatusCommand;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.FileWriterSession;
import io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter.OfflineWriterSessionStatus;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.Map;
import java.util.function.Supplier;

public record OfflineGetStatusOperation(
    CommandContext ctx,
    OfflineGetStatusCommand command,
    Shredder shredder,
    ObjectMapper objectMapper)
    implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    FileWriterSession fileWriterSession =
        (FileWriterSession) queryExecutor.getCqlSessionCache().getSession();
    if (fileWriterSession == null) {
      throw new JsonApiException(
          ErrorCode.OFFLINE_WRITER_SESSION_NOT_FOUND,
          ErrorCode.OFFLINE_WRITER_SESSION_NOT_FOUND.getMessage() + command.sessionId());
    }
    OfflineWriterSessionStatus offlineWriterSessionStatus =
        new OfflineWriterSessionStatus(
            command().sessionId(),
            fileWriterSession.getNamespace(),
            fileWriterSession.getCollection());
    CommandResult commandResult =
        new CommandResult(
            Map.of(CommandStatus.OFFLINE_WRITER_SESSION_STATUS, offlineWriterSessionStatus));
    return Uni.createFrom().item(() -> () -> commandResult);
  }
}
