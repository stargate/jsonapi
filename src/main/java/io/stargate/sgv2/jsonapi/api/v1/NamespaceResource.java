package io.stargate.sgv2.jsonapi.api.v1;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.NamespaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import javax.inject.Inject;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameters;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.resteasy.reactive.RestResponse;

@Path(NamespaceResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = "Namespaces")
public class NamespaceResource {

  public static final String BASE_PATH = "/v1/{namespace}";

  private final CommandProcessor commandProcessor;

  @Inject
  public NamespaceResource(CommandProcessor commandProcessor) {
    this.commandProcessor = commandProcessor;
  }

  @Operation(
      summary = "Execute command",
      description = "Executes a single command against a collection.")
  @Parameters(value = {@Parameter(name = "namespace", ref = "namespace")})
  @RequestBody(
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema = @Schema(anyOf = {CreateCollectionCommand.class}),
              examples = {
                @ExampleObject(ref = "createCollection"),
                @ExampleObject(ref = "findCollections"),
                @ExampleObject(ref = "deleteCollection"),
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
                    @ExampleObject(ref = "resultFindCollections"),
                    @ExampleObject(ref = "resultError"),
                  })))
  @POST
  public Uni<RestResponse<CommandResult>> postCommand(
      @NotNull @Valid NamespaceCommand command, @PathParam("namespace") String namespace) {

    // create context
    CommandContext commandContext = new CommandContext(namespace, null);

    // call processor
    return commandProcessor
        .processCommand(commandContext, command)
        // map to 2xx always
        .map(RestResponse::ok);
  }
}
