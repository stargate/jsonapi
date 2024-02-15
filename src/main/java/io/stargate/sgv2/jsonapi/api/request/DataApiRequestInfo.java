package io.stargate.sgv2.jsonapi.api.request;

import io.stargate.sgv2.api.common.tenant.TenantResolver;
import io.stargate.sgv2.api.common.token.CassandraTokenResolver;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.RequestScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.SecurityContext;
import java.util.Optional;

/**
 * This class is used to get the request info like tenantId and cassandraToken. This is a
 * replacement to DataApiRequestInfo so bridge connection is removed.
 */
@RequestScoped
public class DataApiRequestInfo {
  private final Optional<String> tenantId;
  private final Optional<String> cassandraToken;

  public DataApiRequestInfo(Optional<String> tenantId, Optional<String> cassandraToken) {
    this.tenantId = tenantId;
    this.cassandraToken = cassandraToken;
  }

  @Inject
  public DataApiRequestInfo(
      RoutingContext routingContext,
      SecurityContext securityContext,
      Instance<TenantResolver> tenantResolver,
      Instance<CassandraTokenResolver> tokenResolver) {
    this.tenantId =
        ((TenantResolver) tenantResolver.get()).resolve(routingContext, securityContext);
    this.cassandraToken =
        ((CassandraTokenResolver) tokenResolver.get()).resolve(routingContext, securityContext);
  }

  public Optional<String> getTenantId() {
    return this.tenantId;
  }

  public Optional<String> getCassandraToken() {
    return this.cassandraToken;
  }
}
