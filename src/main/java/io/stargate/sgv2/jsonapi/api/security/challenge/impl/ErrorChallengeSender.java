package io.stargate.sgv2.jsonapi.api.security.challenge.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.security.challenge.ChallengeSender;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.config.constants.HttpConstants;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Responds with {@link CommandResult} containing an error on send challenge. */
@ApplicationScoped
// TODO: create Data API ChallengerSender when remove quarkus-common dependency
public class ErrorChallengeSender implements ChallengeSender {

  private static final Logger LOG = LoggerFactory.getLogger(ErrorChallengeSender.class);

  /** Object mapper for custom response. */
  private final ObjectMapper objectMapper;

  /** Result is always constant */
  private final CommandResult commandResult;

  @Inject
  public ErrorChallengeSender(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    String message =
        "Role unauthorized for operation: Missing token, expecting one in the %s header."
            .formatted(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME);
    CommandResult.Error error =
        new CommandResult.Error(
            message, Collections.emptyMap(), Collections.emptyMap(), Response.Status.UNAUTHORIZED);
    commandResult = new CommandResult(List.of(error));
  }

  /** {@inheritDoc} */
  @Override
  public Uni<Boolean> apply(RoutingContext context, ChallengeData challengeData) {
    try {
      // try to serialize
      String response = objectMapper.writeValueAsString(commandResult);

      // set content type
      context.response().headers().set(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);

      // set content length
      context
          .response()
          .headers()
          .set(HttpHeaders.CONTENT_LENGTH, String.valueOf(response.getBytes().length));

      // Return the status code from the challenge data
      context.response().setStatusCode(challengeData.status);

      // write and map to true
      return Uni.createFrom()
          .completionStage(context.response().write(response).map(true).toCompletionStage());
    } catch (JsonProcessingException e) {
      LOG.error("Unable to serialize CommandResult instance {} to JSON.", commandResult, e);

      // keep original status in case we failed
      context.response().setStatusCode(challengeData.status);
      return Uni.createFrom().item(true);
    }
  }
}
