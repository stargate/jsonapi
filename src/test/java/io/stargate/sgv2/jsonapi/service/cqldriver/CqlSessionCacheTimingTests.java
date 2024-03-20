package io.stargate.sgv2.jsonapi.service.cqldriver;

import static io.stargate.sgv2.jsonapi.service.cqldriver.TenantAwareCqlSessionBuilderTest.TENANT_ID_PROPERTY_KEY;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(FixedTokenTimingTestProfile.class)
public class CqlSessionCacheTimingTests {

  @Inject OperationsConfig operationsConfig;

  private MeterRegistry meterRegistry;

  /**
   * List of sessions created in the tests. This is used to close the sessions after each test. This
   * is needed because, though the sessions evicted from the cache are closed, the sessions left
   * active on the cache are not closed, so we have to close them explicitly.
   */
  private List<CqlSession> sessionsCreatedInTests;

  @BeforeEach
  public void setupEachTest() {
    meterRegistry = new SimpleMeterRegistry();
    sessionsCreatedInTests = new ArrayList<>();
  }

  @AfterEach
  public void tearDownEachTest() {
    sessionsCreatedInTests.forEach(CqlSession::close);
  }

  @Test
  public void testOSSCxCQLSessionCacheTimedEviction() throws InterruptedException {
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    int sessionsToCreate = operationsConfig.databaseConfig().sessionCacheMaxSize();
    for (int i = 0; i < sessionsToCreate; i++) {
      String tenantId = "tenant_timing_test_" + i;
      DataApiRequestInfo dataApiRequestInfo = mock(DataApiRequestInfo.class);
      when(dataApiRequestInfo.getTenantId()).thenReturn(Optional.of(tenantId));
      when(dataApiRequestInfo.getCassandraToken())
          .thenReturn(operationsConfig.databaseConfig().fixedToken());
      CqlSession cqlSession = cqlSessionCacheForTest.getSession(dataApiRequestInfo);
      sessionsCreatedInTests.add(cqlSession);
      assertThat(
              ((DefaultDriverContext) cqlSession.getContext())
                  .getStartupOptions()
                  .get(TENANT_ID_PROPERTY_KEY))
          .isEqualTo(tenantId);
    }
    Thread.sleep(10000);
    assertThat(cqlSessionCacheForTest.cacheSize()).isEqualTo(0);
    // metrics test
    Gauge cacheSizeMetric =
        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value()).isEqualTo(0);
    FunctionCounter cachePutMetric =
        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
    assertThat(cachePutMetric).isNotNull();
    assertThat(cachePutMetric.count()).isEqualTo(sessionsToCreate);
    FunctionCounter cacheLoadMetric =
        meterRegistry
            .find("cache.load")
            .tag("cache", "cql_sessions_cache")
            .tag("result", "success")
            .functionCounter();
    assertThat(cacheLoadMetric).isNotNull();
    assertThat(cacheLoadMetric.count()).isEqualTo(sessionsToCreate);
  }
}
