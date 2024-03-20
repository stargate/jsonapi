package io.stargate.sgv2.jsonapi.service.cqldriver;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlSession;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.exception.mappers.ThrowableToErrorMapper;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(InvalidCredentialsProfile.class)
public class InvalidCredentialsTests {

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
  public void testOSSCxCQLSessionCacheWithInvalidCredentials()
      throws NoSuchFieldException, IllegalAccessException {
    // set request info
    DataApiRequestInfo dataApiRequestInfo = mock(DataApiRequestInfo.class);
    when(dataApiRequestInfo.getTenantId()).thenReturn(Optional.of(TENANT_ID_FOR_TEST));
    when(dataApiRequestInfo.getCassandraToken())
        .thenReturn(operationsConfig.databaseConfig().fixedToken());
    CQLSessionCache cqlSessionCacheForTest = new CQLSessionCache(operationsConfig, meterRegistry);
    // Throwable
    Throwable t = null;
    try {
      CqlSession cqlSession = cqlSessionCacheForTest.getSession(dataApiRequestInfo);
    } catch (Throwable throwable) {
      t = throwable;
    }
    assertThat(t).isInstanceOf(AllNodesFailedException.class);
    CommandResult.Error error =
        ThrowableToErrorMapper.getMapperWithMessageFunction().apply(t, t.getMessage());
    assertThat(error).isNotNull();
    assertThat(error.message()).contains("UNAUTHENTICATED: Invalid token");
    assertThat(error.status()).isEqualTo(Response.Status.UNAUTHORIZED);
    assertThat(cqlSessionCacheForTest.cacheSize()).isEqualTo(0);
    // metrics test
    Gauge cacheSizeMetric =
        meterRegistry.find("cache.size").tag("cache", "cql_sessions_cache").gauge();
    assertThat(cacheSizeMetric).isNotNull();
    assertThat(cacheSizeMetric.value()).isEqualTo(0);
    FunctionCounter cachePutMetric =
        meterRegistry.find("cache.puts").tag("cache", "cql_sessions_cache").functionCounter();
    assertThat(cachePutMetric).isNotNull();
    assertThat(cachePutMetric.count()).isEqualTo(1);
    FunctionCounter cacheLoadSuccessMetric =
        meterRegistry
            .find("cache.load")
            .tag("cache", "cql_sessions_cache")
            .tag("result", "success")
            .functionCounter();
    assertThat(cacheLoadSuccessMetric).isNotNull();
    assertThat(cacheLoadSuccessMetric.count()).isEqualTo(0);
    FunctionCounter cacheLoadFailureMetric =
        meterRegistry
            .find("cache.load")
            .tag("cache", "cql_sessions_cache")
            .tag("result", "failure")
            .functionCounter();
    assertThat(cacheLoadFailureMetric).isNotNull();
    assertThat(cacheLoadFailureMetric.count()).isEqualTo(1);
  }
}
