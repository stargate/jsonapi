package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.request.FileWriterParams;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.InsertOperation;
import jakarta.inject.Inject;
import java.util.List;
import java.util.UUID;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Representation of the offline begin-writer API {@link Command}. */
@Schema(description = "Command that initializes the offline writer.")
@JsonTypeName("beginOfflineSession")
public class BeginOfflineSessionCommand implements CollectionCommand {
  @Schema(
      description = "The namespace to write to.",
      type = SchemaType.STRING,
      implementation = String.class)
  private final String namespace;

  @Schema(
      name = "createCollection",
      description = "Create collection command.",
      type = SchemaType.OBJECT,
      implementation = CreateCollectionCommand.class)
  private final CreateCollectionCommand createCollection;

  @Schema(
      description = "The SSTable output directory.",
      type = SchemaType.STRING,
      implementation = String.class)
  private final String ssTableOutputDirectory;

  @Schema(
      description = "The file writer buffer size.",
      type = SchemaType.INTEGER,
      implementation = Integer.class)
  private final int fileWriterBufferSizeInMB;

  @JsonIgnore private final CollectionSettings collectionSettings;
  @JsonIgnore private final EmbeddingService embeddingService;
  @JsonIgnore private final String sessionId;
  @JsonIgnore private final FileWriterParams fileWriterParams;

  /**
   * fileWriterBufferSize Constructs a new {@link BeginOfflineSessionCommand}.
   *
   * @param namespace the namespace to write to
   * @param createCollection the create collection command
   * @param ssTableOutputDirectory the SSTable output directory
   */
  @Inject
  public BeginOfflineSessionCommand(
      String namespace,
      CreateCollectionCommand createCollection,
      String ssTableOutputDirectory,
      int fileWriterBufferSizeInMB) {
    this.namespace = namespace;
    this.createCollection = createCollection;
    this.ssTableOutputDirectory = ssTableOutputDirectory;
    this.collectionSettings = buildCollectionSettings();
    this.embeddingService = null; // TODO-SL
    this.sessionId = UUID.randomUUID().toString();
    this.fileWriterBufferSizeInMB = fileWriterBufferSizeInMB;
    this.fileWriterParams = buildFileWriterParams();
  }

  private CollectionSettings buildCollectionSettings() {
    boolean isVectorEnabled =
        this.createCollection.options() != null && this.createCollection.options().vector() != null;
    int vectorSize = isVectorEnabled ? this.createCollection.options().vector().dimension() : 0;
    CollectionSettings.SimilarityFunction similarityFunction =
        isVectorEnabled
            ? CollectionSettings.SimilarityFunction.fromString(
                this.createCollection.options().vector().metric())
            : null;

    return CollectionSettings.getCollectionSettings(
        this.createCollection.name(),
        isVectorEnabled,
        vectorSize,
        similarityFunction,
        null, // TODO
        new ObjectMapper()); // TODO
  }

  private FileWriterParams buildFileWriterParams() {
    CreateCollectionOperation createCollectionOperation =
        CreateCollectionOperation.forCQL(
            this.collectionSettings.vectorEnabled(),
            this.collectionSettings.vectorEnabled()
                ? this.collectionSettings.similarityFunction().toString()
                : null,
            this.collectionSettings.vectorEnabled() ? this.collectionSettings.vectorSize() : 0,
            null); // TODO-SL fix comments field
    String createTableCQL =
        createCollectionOperation
            .getCreateTable(this.namespace, this.createCollection.name())
            .getQuery();
    List<String> indexCQLs =
        createCollectionOperation
            .getIndexStatements(this.namespace, this.createCollection.name())
            .stream()
            .map(SimpleStatement::getQuery)
            .toList();
    String insertStatementCQL =
        InsertOperation.forCQL(new CommandContext(this.namespace, this.createCollection.name()))
            .buildInsertQuery(
                this.collectionSettings.vectorEnabled()); // TODO-SL add conditionalInsert to method
    return new FileWriterParams(
        this.namespace,
        this.getCreateCollectionCommand().name(),
        this.getSsTableOutputDirectory(),
        fileWriterBufferSizeInMB,
        createTableCQL,
        insertStatementCQL,
        indexCQLs,
        this.collectionSettings.vectorEnabled());
  }

  public String getNamespace() {
    return namespace;
  }

  public CreateCollectionCommand getCreateCollectionCommand() {
    return createCollection;
  }

  public String getSsTableOutputDirectory() {
    return ssTableOutputDirectory;
  }

  public CollectionSettings getCollectionSettings() {
    return this.collectionSettings;
  }

  public EmbeddingService getEmbeddingService() {
    return this.embeddingService;
  }

  public String getSessionId() {
    return this.sessionId;
  }

  public FileWriterParams getFileWriterParams() {
    return this.fileWriterParams;
  }
}
