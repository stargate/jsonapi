package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Representation of the offline end-writer API {@link Command}. */
@Schema(description = "Command that initializes the offline writer.")
@JsonTypeName("offlineEndWriter")
public record OfflineEndWriterCommand(
    @Schema(
            description = "The session ID to end",
            type = SchemaType.STRING,
            implementation = String.class)
        String sessionId)
    implements Command {}
