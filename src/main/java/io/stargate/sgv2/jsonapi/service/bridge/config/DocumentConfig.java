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

package io.stargate.sgv2.jsonapi.service.bridge.config;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.stargate.sgv2.jsonapi.config.LwtConfig;

import javax.validation.constraints.Max;
import javax.validation.constraints.Positive;

/** Configuration for the documents. */
@ConfigMapping(prefix = "stargate.document")
public interface DocumentConfig {

  /** @return Defines the default document page size, defaults to <code>20</code>. */
  @Max(500)
  @Positive
  @WithDefault("20")
  int defaultPageSize();

  /**
   * @return Defines the default document page size for sorting, having separate config because sort
   *     will more rows in per page, defaults to <code>100</code>.
   */
  @Max(500)
  @Positive
  @WithDefault("100")
  int defaultSortPageSize();

  /**
   * @return Defines the maximum limit of document that can be returned for a request, defaults to
   *     <code>1000</code>.
   */
  @Max(Integer.MAX_VALUE)
  @Positive
  @WithDefault("1000")
  int maxLimit();

  /**
   * @return Defines the maximum limit of document read to perform in memory sorting <code>10000
   *     </code>.
   */
  @Max(10000)
  @Positive
  @WithDefault("10000")
  int maxSortReadLimit();

  /**
   * @return Defines the maximum limit of document that can be deleted for a request, defaults to
   *     <code>20</code>.
   */
  @Max(100)
  @Positive
  @WithDefault("20")
  int maxDocumentDeleteCount();

  /**
   * @return Defines the maximum limit of document that can be updated for a request, defaults to
   *     <code>20</code>.
   */
  @Max(100)
  @Positive
  @WithDefault("20")
  int maxDocumentUpdateCount();


}
