package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Implementation of {@code $unset} update operation used to remove fields from documents. */
public class UnsetOperation extends UpdateOperation {
  private List<UnsetAction> actions;

  private UnsetOperation(List<UnsetAction> actions) {
    this.actions = sortByPath(actions);
  }

  public static UnsetOperation construct(ObjectNode args) {
    Iterator<String> it = args.fieldNames();
    List<UnsetAction> actions = new ArrayList<>();
    while (it.hasNext()) {
      actions.add(
          new UnsetAction(
              ActionTargetLocator.forPath(validateUpdatePath(UpdateOperator.UNSET, it.next()))));
    }
    return new UnsetOperation(actions);
  }

  @Override
  public boolean updateDocument(ObjectNode doc) {
    boolean modified = false;
    for (UnsetAction action : actions) {
      ActionTarget target = action.target().findIfExists(doc);
      modified |= (target.removeValue() != null);
    }
    return modified;
  }

  public Set<String> getPaths() {
    return actions.stream().map(UnsetAction::path).collect(Collectors.toSet());
  }

  private record UnsetAction(ActionTargetLocator target) implements ActionWithTarget {}
}
