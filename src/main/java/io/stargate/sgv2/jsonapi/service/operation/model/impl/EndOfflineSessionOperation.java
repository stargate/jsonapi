package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.EndOfflineSessionCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import java.util.Map;
import java.util.function.Supplier;

public record EndOfflineSessionOperation(
    CommandContext ctx,
    EndOfflineSessionCommand command,
    Shredder shredder,
    ObjectMapper objectMapper)
    implements Operation {
  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    CqlSession session = queryExecutor.getCqlSessionCache().getSession(dataApiRequestInfo);
    session.close();
    CommandResult commandResult =
        new CommandResult(
            Map.of(
                CommandStatus.OFFLINE_WRITER_SESSION_ID,
                command.sessionId(),
                CommandStatus.OK,
                true));
    return Uni.createFrom().item(() -> () -> commandResult);
  }
}
