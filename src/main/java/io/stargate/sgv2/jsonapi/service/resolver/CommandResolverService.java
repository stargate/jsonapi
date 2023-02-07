package io.stargate.sgv2.jsonapi.service.resolver;

import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;

/** Service to map each available {@link CommandResolver}. */
@ApplicationScoped
public class CommandResolverService {

  private final Map<Class<? extends Command>, CommandResolver<? extends Command>> resolvers;

  @Inject
  public CommandResolverService(Instance<CommandResolver<? extends Command>> commandResolvers) {
    this(commandResolvers.stream().toList());
  }

  public CommandResolverService(List<CommandResolver<? extends Command>> commandResolvers) {
    this.resolvers = new HashMap<>();
    if (null != commandResolvers) {
      commandResolvers.forEach(resolver -> resolvers.put(resolver.getCommandClass(), resolver));
    }
  }

  /**
   * @param command {@link Command}
   * @return Return s the {@link CommandResolver} for the given command if one exists, otherwise
   *     throwns a {@link DocsException}.
   * @param <T> Type of the command
   */
  public <T extends Command> CommandResolver<T> resolverForCommand(T command) {
    // try to get from the map of resolvers
    CommandResolver<T> resolver = (CommandResolver<T>) resolvers.get(command.getClass());

    // if this results to null, fail here with not implemented
    if (null == resolver) {
      String msg =
          "The command %s is not implemented.".formatted(command.getClass().getSimpleName());
      throw new JsonApiException(ErrorCode.COMMAND_NOT_IMPLEMENTED, msg);
    } else {
      return resolver;
    }
  }
}
