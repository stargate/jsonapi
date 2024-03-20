/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.jsonapi.api.request.token.configuration;

import io.quarkus.arc.lookup.LookupIfProperty;
import io.stargate.sgv2.jsonapi.api.request.token.DataApiTokenResolver;
import io.stargate.sgv2.jsonapi.api.request.token.impl.FixedTokenResolver;
import io.stargate.sgv2.jsonapi.api.request.token.impl.HeaderTokenResolver;
import io.stargate.sgv2.jsonapi.api.request.token.impl.PrincipalTokenResolver;
import io.stargate.sgv2.jsonapi.config.AuthConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import java.util.Optional;

/** Configuration for activating a correct {@link CassandraTokenResolver}. */
public class DataApiTokenConfiguration {

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.auth.token-resolver.type", stringValue = "header")
  DataApiTokenResolver headerTokenResolver(AuthConfig config) {
    String headerName = config.tokenResolver().header().headerName();
    return new HeaderTokenResolver(headerName);
  }

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.auth.token-resolver.type", stringValue = "principal")
  DataApiTokenResolver principalTokenResolver() {
    return new PrincipalTokenResolver();
  }

  @Produces
  @ApplicationScoped
  @LookupIfProperty(name = "stargate.auth.token-resolver.type", stringValue = "fixed")
  DataApiTokenResolver fixedTokenResolver(AuthConfig config) {
    return new FixedTokenResolver(config.tokenResolver().fixed());
  }

  @Produces
  @ApplicationScoped
  @LookupIfProperty(
      name = "stargate.auth.token-resolver.type",
      stringValue = "noop",
      lookupIfMissing = true)
  DataApiTokenResolver noopCassandraTokenResolver() {
    return (context, securityContext) -> Optional.empty();
  }
}
