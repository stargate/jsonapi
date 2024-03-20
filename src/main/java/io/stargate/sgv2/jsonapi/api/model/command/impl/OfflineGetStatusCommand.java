package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonTypeName;
import io.stargate.sgv2.jsonapi.api.model.command.CollectionCommand;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/** Representation of the offline get status API {@link Command}. */
@Schema(description = "Command that gets the status of the offline writer.")
@JsonTypeName("offlineGetStatus")
public record OfflineGetStatusCommand(
    @Schema(
            description = "The session ID to get the status of",
            type = SchemaType.STRING,
            implementation = String.class)
        String sessionId)
    implements CollectionCommand {}
