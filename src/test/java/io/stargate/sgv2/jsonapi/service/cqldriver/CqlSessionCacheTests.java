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
import io.quarkus.security.UnauthorizedException;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import jakarta.inject.Inject;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(FixedTokenOverrideProfile.class)
public class CqlSessionCacheTests {

  private static final String TENANT_ID_FOR_TEST = "test_tenant";

  @Inject OperationsConfig operationsConfig;

  private MeterRegistry meterRegistry;

  /**
   * List of sessions created in the tests. This is used to close the sessions after each test. This
   * is needed because, though the sessions evicted from the cache are closed, the sessions left
   * active on the cache are not closed, so we have to close them explicitly.
   */
  private List<CqlSession> sessionsCreatedInTests;

  @BeforeEach
  public void tearUpEachTest() {
    meterRegistry = new SimpleMeterRegistry();
    sessionsCreatedInTests = new ArrayList<>();
  }

  @AfterEach
  public void tearDownEachTest() {
    sessionsCreatedInTests.forEach(CqlSession::close);
  }

  @Test
  public void testOSSCxCQLSessionCacheDefaultTenant() {
    DataApiRequestInfo dataApiRequestInfo = mock(DataApiRequestInfo.class);
    when(dataApiRequestInfo.getCassandraToken())
        .thenReturn(operationsConfig.databaseConfig().fixedToken());
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    CqlSession cqlSession = cqlSessionCacheForTest.getSession(dataApiRequestInfo);
    sessionsCreatedInTests.add(cqlSession);
    assertThat(
            ((DefaultDriverContext) cqlSession.getContext())
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo("default_tenant");
    Gauge cacheSizeMetric =
        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    FunctionCounter cachePutMetric =
        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value()).isEqualTo(1);
    assertThat(cachePutMetric).isNotNull();
    assertThat(cachePutMetric.count()).isEqualTo(1);
  }

  @Test
  public void testOSSCxCQLSessionCacheWithFixedToken()
      throws NoSuchFieldException, IllegalAccessException {
    // set request info
    DataApiRequestInfo dataApiRequestInfo = mock(DataApiRequestInfo.class);
    when(dataApiRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    when(dataApiRequestInfo.getCassandraToken())
        .thenReturn(operationsConfig.databaseConfig().fixedToken());
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    // set operation config
    Field operationsConfigField =
        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
    operationsConfigField.setAccessible(true);
    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
    CqlSession cqlSession = cqlSessionCacheForTest.getSession(dataApiRequestInfo);
    sessionsCreatedInTests.add(cqlSession);
    assertThat(
            ((DefaultDriverContext) cqlSession.getContext())
                .getStartupOptions()
                .get(TENANT_ID_PROPERTY_KEY))
        .isEqualTo(TENANT_ID_FOR_TEST);
    // metrics test
    Gauge cacheSizeMetric =
        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    FunctionCounter cachePutMetric =
        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value()).isEqualTo(1);
    assertThat(cachePutMetric).isNotNull();
    assertThat(cachePutMetric.count()).isEqualTo(1);
  }

  @Test
  public void testOSSCxCQLSessionCacheWithInvalidFixedToken()
      throws NoSuchFieldException, IllegalAccessException {
    // set request info
    DataApiRequestInfo dataApiRequestInfo = mock(DataApiRequestInfo.class);
    when(dataApiRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    when(dataApiRequestInfo.getCassandraToken()).thenReturn(Optional.of("invalid_token"));
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    // set operation config
    Field operationsConfigField =
        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
    operationsConfigField.setAccessible(true);
    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
    // Throwable
    Throwable t = null;
    try {
      CqlSession cqlSession = cqlSessionCacheForTest.getSession(dataApiRequestInfo);
    } catch (Throwable throwable) {
      t = throwable;
    }
    assertThat(t).isNotNull().isInstanceOf(UnauthorizedException.class).hasMessage("Unauthorized");
    // metrics test
    Gauge cacheSizeMetric =
        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    FunctionCounter cachePutMetric =
        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value()).isEqualTo(0);
    assertThat(cachePutMetric).isNotNull();
    assertThat(cachePutMetric.count()).isEqualTo(0);
  }

  @Test
  public void testOSSCxCQLSessionCacheMultiTenant()
      throws NoSuchFieldException, IllegalAccessException {
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    // set operation config
    Field operationsConfigField =
        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
    operationsConfigField.setAccessible(true);
    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
    List<String> tenantIds = new ArrayList<>();
    tenantIds.add("tenant1");
    tenantIds.add("tenant2");
    tenantIds.add("tenant3");
    tenantIds.add("tenant4");
    tenantIds.add("tenant5");
    for (String tenantId : tenantIds) {
      // set request info
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
    // metrics test
    Gauge cacheSizeMetric =
        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value()).isEqualTo(tenantIds.size());
    FunctionCounter cachePutMetric =
        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
    assertThat(cachePutMetric).isNotNull();
    assertThat(cachePutMetric.count()).isEqualTo(tenantIds.size());
    FunctionCounter cacheLoadMetric =
        meterRegistry
            .find("cache.load")
            .tag("cache", "cql_sessions_cache")
            .tag("result", "success")
            .functionCounter();
    assertThat(cacheLoadMetric).isNotNull();
    assertThat(cacheLoadMetric.count()).isEqualTo(tenantIds.size());
  }

  @Test
  public void testOSSCxCQLSessionCacheSizeEviction()
      throws NoSuchFieldException, IllegalAccessException {
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    // set operation config
    Field operationsConfigField =
        cqlSessionCacheForTest.getClass().getDeclaredField("operationsConfig");
    operationsConfigField.setAccessible(true);
    operationsConfigField.set(cqlSessionCacheForTest, operationsConfig);
    int sessionsToCreate = operationsConfig.databaseConfig().sessionCacheMaxSize() + 5;
    for (int i = 0; i < sessionsToCreate; i++) {
      String tenantId = "tenant" + i;
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
    // test if cache size is maintained at `sessionCacheMaxSizeConfigured`
    long cacheSize = cqlSessionCacheForTest.cacheSize();
    assertThat(cacheSize).isEqualTo(operationsConfig.databaseConfig().sessionCacheMaxSize());
    // metrics test
    Gauge cacheSizeMetric =
        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value())
        .isEqualTo(operationsConfig.databaseConfig().sessionCacheMaxSize());
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
