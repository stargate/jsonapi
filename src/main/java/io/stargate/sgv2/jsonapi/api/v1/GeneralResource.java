package io.stargate.sgv2.jsonapi.api.v1;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.GeneralCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateEmbeddingServiceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateNamespaceCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

@Path(GeneralResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = "General")
public class GeneralResource {

  public static final String BASE_PATH = "/v1";

  private final MeteredCommandProcessor meteredCommandProcessor;

  @Inject private DataApiRequestInfo dataApiRequestInfo;

  @Inject
  public GeneralResource(MeteredCommandProcessor meteredCommandProcessor) {
    this.meteredCommandProcessor = meteredCommandProcessor;
  }

  @Operation(summary = "Execute command", description = "Executes a single general command.")
  @RequestBody(
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      anyOf = {CreateEmbeddingServiceCommand.class, CreateNamespaceCommand.class}),
              examples = {
                @ExampleObject(ref = "createEmbeddingService"),
                @ExampleObject(ref = "createNamespace"),
                @ExampleObject(ref = "createNamespaceWithReplication"),
                @ExampleObject(ref = "findNamespaces"),
                @ExampleObject(ref = "dropNamespace"),
              }))
  @APIResponses(
      @APIResponse(
          responseCode = "200",
          description =
              "Call successful. Returns result of the command execution. Note that in case of errors, response code remains `HTTP 200`.",
          content =
              @Content(
                  mediaType = MediaType.APPLICATION_JSON,
                  schema = @Schema(implementation = CommandResult.class),
                  examples = {
                    @ExampleObject(ref = "resultCreate"),
                    @ExampleObject(ref = "resultFindNamespaces"),
                    @ExampleObject(ref = "resultError"),
                  })))
  @POST
  public Uni<RestResponse<CommandResult>> postCommand(@NotNull @Valid GeneralCommand command) {
    // call processor
    return meteredCommandProcessor
        .processCommand(dataApiRequestInfo, CommandContext.empty(), command) // TODO-SL
        // map to 2xx unless overridden by error
        .map(commandResult -> commandResult.map());
  }
}
