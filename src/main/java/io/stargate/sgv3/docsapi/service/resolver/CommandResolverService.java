package io.stargate.sgv3.docsapi.service.resolver;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv3.docsapi.api.model.command.Command;
import io.stargate.sgv3.docsapi.service.resolver.model.CommandResolver;
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
   * @return Emits the {@link CommandResolver} for the given command if one exists, otherwise emits
   *     a null item.
   * @param <T> Type of the command
   */
  public <T extends Command> Uni<CommandResolver<T>> resolverForCommand(T command) {
    // TODO should we already emit an error on null?
    return Uni.createFrom().item((CommandResolver<T>) resolvers.get(command.getClass()));
  }
}
