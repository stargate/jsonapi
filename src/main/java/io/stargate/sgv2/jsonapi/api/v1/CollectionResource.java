package io.stargate.sgv2.jsonapi.api.v1;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
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
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableCommandResultSupplier;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaCache;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingServiceCache;
import io.stargate.sgv2.jsonapi.service.processor.MeteredCommandProcessor;
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

  private final MeteredCommandProcessor meteredCommandProcessor;

  @Inject private SchemaCache schemaCache;

  @Inject private EmbeddingServiceCache serviceCache;

  @Inject private DataApiRequestInfo dataApiRequestInfo;

  @Inject private JsonProcessingMetricsReporter jsonProcessingMetricsReporter;

  @Inject
  public CollectionResource(MeteredCommandProcessor meteredCommandProcessor) {
    this.meteredCommandProcessor = meteredCommandProcessor;
  }

  @Operation(
      summary = "Execute command",
      description = "Executes a single command against a collection.",
      operationId = "collection operation")
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
                        CountDocumentsCommand.class,
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
                @ExampleObject(ref = "findVectorSearch"),
                @ExampleObject(ref = "findOne"),
                @ExampleObject(ref = "findOneVectorSearch"),
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
    return schemaCache
        .getCollectionSettings(dataApiRequestInfo.getTenantId(), namespace, collection)
        .onItemOrFailure()
        .transformToUni(
            (collectionProperty, throwable) -> {
              if (throwable != null) {
                Throwable error = throwable;
                if (throwable instanceof RuntimeException && throwable.getCause() != null)
                  error = throwable.getCause();
                else if (error instanceof JsonApiException jsonApiException) {
                  return Uni.createFrom().failure(jsonApiException);
                }
                // otherwise use generic for now
                return Uni.createFrom().item(new ThrowableCommandResultSupplier(error));
              } else {
                EmbeddingService embeddingService = null;
                if (collectionProperty.vectorizeServiceName() != null
                    && collectionProperty.modelName() != null) {
                  embeddingService =
                      serviceCache.getConfiguration(
                          dataApiRequestInfo.getTenantId(),
                          collectionProperty.vectorizeServiceName(),
                          collectionProperty.modelName());
                }

                CommandContext commandContext =
                    new CommandContext(
                        namespace,
                        collection,
                        collectionProperty,
                        embeddingService,
                        command.getClass().getSimpleName(),
                        jsonProcessingMetricsReporter);

                // call processor
                return meteredCommandProcessor.processCommand(commandContext, command);
              }
            })
        .map(commandResult -> commandResult.map());
  }
}
