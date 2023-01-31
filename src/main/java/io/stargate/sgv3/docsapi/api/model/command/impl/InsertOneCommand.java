package io.stargate.sgv3.docsapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.api.model.command.ModifyCommand;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Representation of the insertOne API {@link Command}.
 *
 * @param document The document to insert.
 */
@Schema(description = "Command that inserts a single JSON document to a collection.")
@JsonTypeName("insertOne")
public record InsertOneCommand(
    @NotNull
        @Schema(
            description = "JSON document to insert.",
            implementation = Object.class,
            type = SchemaType.OBJECT)
        JsonNode document,
    @Nullable @JsonProperty("options") Options options)
    implements ModifyCommand {
  public record Options() {}
}
