package io.stargate.sgv2.jsonapi.api.v1;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommands;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.DeleteOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndDeleteCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndReplaceCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneAndUpdateCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.InsertOneCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateManyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.UpdateOneCommand;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
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

@Path(CollectionResource.BASE_PATH)
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@SecurityRequirement(name = OpenApiConstants.SecuritySchemes.TOKEN)
@Tag(ref = "Documents")
public class CollectionResource {

  public static final String BASE_PATH = "/v1/{namespace}/{collection}";

  private final CommandProcessor commandProcessor;

  @Inject
  public CollectionResource(CommandProcessor commandProcessor) {
    this.commandProcessor = commandProcessor;
  }

  @Operation(
      summary = "Execute command",
      description = "Executes a single command against a collection.")
  @Parameters(
      value = {
        @Parameter(name = "namespace", ref = "namespace"),
        @Parameter(name = "collection", ref = "collection")
      })
  @RequestBody(
      content =
          @Content(
              mediaType = MediaType.APPLICATION_JSON,
              schema =
                  @Schema(
                      anyOf = {
                        CountDocumentsCommands.class,
                        DeleteOneCommand.class,
                        DeleteManyCommand.class,
                        FindOneCommand.class,
                        FindCommand.class,
                        FindOneAndDeleteCommand.class,
                        FindOneAndReplaceCommand.class,
                        FindOneAndUpdateCommand.class,
                        InsertOneCommand.class,
                        InsertManyCommand.class,
                        UpdateManyCommand.class,
                        UpdateOneCommand.class
                      }),
              examples = {
                @ExampleObject(ref = "countDocuments"),
                @ExampleObject(ref = "deleteOne"),
                @ExampleObject(ref = "deleteMany"),
                @ExampleObject(ref = "find"),
                @ExampleObject(ref = "findOne"),
                @ExampleObject(ref = "findOneAndDelete"),
                @ExampleObject(ref = "findOneAndReplace"),
                @ExampleObject(ref = "findOneAndUpdate"),
                @ExampleObject(ref = "insertOne"),
                @ExampleObject(ref = "insertMany"),
                @ExampleObject(ref = "updateMany"),
                @ExampleObject(ref = "updateOne"),
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
                    @ExampleObject(ref = "resultCount"),
                    @ExampleObject(ref = "resultDeleteOne"),
                    @ExampleObject(ref = "resultDeleteMany"),
                    @ExampleObject(ref = "resultFind"),
                    @ExampleObject(ref = "resultFindOne"),
                    @ExampleObject(ref = "resultFindOneAndDelete"),
                    @ExampleObject(ref = "resultFindOneAndReplace"),
                    @ExampleObject(ref = "resultFindOneAndUpdate"),
                    @ExampleObject(ref = "resultFindOneAndUpdateUpsert"),
                    @ExampleObject(ref = "resultInsert"),
                    @ExampleObject(ref = "resultUpdateOne"),
                    @ExampleObject(ref = "resultUpdateOneUpsert"),
                    @ExampleObject(ref = "resultUpdateMany"),
                    @ExampleObject(ref = "resultUpdateManyUpsert"),
                    @ExampleObject(ref = "resultError"),
                  })))
  @POST
  public Uni<RestResponse<CommandResult>> postCommand(
      @NotNull @Valid CollectionCommand command,
      @PathParam("namespace")
          @NotNull
          @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
          @Size(min = 1, max = 48)
          String namespace,
      @PathParam("collection")
          @NotNull
          @Pattern(regexp = "[a-zA-Z][a-zA-Z0-9_]*")
          @Size(min = 1, max = 48)
          String collection) {

    // create context
    CommandContext commandContext = new CommandContext(namespace, collection);

    // call processor
    return commandProcessor
        .processCommand(commandContext, command)
        // map to 2xx unless overridden by error
        .map(commandResult -> commandResult.map());
  }
}
