package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.api.model.command.ModifyCommand;
import io.stargate.sgv2.jsonapi.api.model.command.validation.MaxInsertManyDocuments;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Representation of the offline insertMany API {@link Command}. */
@Schema(description = "Command that inserts multiple JSON document to a collection.")
@JsonTypeName("offlineInsertMany")
public record OfflineInsertManyCommand(
    @NotNull
        @Schema(
            description = "The session id of the writer.",
            implementation = String.class,
            type = SchemaType.STRING)
        String writerSessionId,
    @NotNull
        @NotEmpty
        @MaxInsertManyDocuments
        @Schema(
            description = "JSON document to insert.",
            implementation = Object.class,
            type = SchemaType.ARRAY)
        List<JsonNode> documents)
    implements ModifyCommand {}
