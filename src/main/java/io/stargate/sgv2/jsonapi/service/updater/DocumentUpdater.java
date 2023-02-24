package io.stargate.sgv2.jsonapi.service.updater;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateClause;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateOperation;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.UpdateTargetLocator;
import java.util.List;

/** Updates the document read from the database with the updates came as part of the request. */
public record DocumentUpdater(List<UpdateOperation> updateOperations) {
  public static DocumentUpdater construct(UpdateClause updateDef) {
    return new DocumentUpdater(updateDef.buildOperations());
  }

  public DocumentUpdaterResponse applyUpdates(JsonNode readDocument) {
    UpdateTargetLocator targetLocator = new UpdateTargetLocator();
    ObjectNode docToUpdate = (ObjectNode) readDocument;
    boolean modified = false;
    for (UpdateOperation updateOperation : updateOperations)
      modified |= updateOperation.updateDocument(docToUpdate, targetLocator);
    return new DocumentUpdaterResponse(readDocument, modified);
  }

  public record DocumentUpdaterResponse(JsonNode document, boolean modified) {}
}
