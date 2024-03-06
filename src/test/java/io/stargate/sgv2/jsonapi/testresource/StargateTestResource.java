//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package io.stargate.sgv2.jsonapi.testresource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.common.DevServicesContext;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

public abstract class StargateTestResource
    implements QuarkusTestResourceLifecycleManager, DevServicesContext.ContextAware {
  private static final Logger LOG = LoggerFactory.getLogger(StargateTestResource.class);

  // Astra will have 8kB limit some time around or after 1.0.0-BETA-6:
  protected static final int DEFAULT_SAI_MAX_STRING_TERM_SIZE_KB = 8;
  private Map<String, String> initArgs;
  protected Optional<String> containerNetworkId;
  private Network network;
  private GenericContainer<?> cassandraContainer;
  private GenericContainer<?> stargateContainer;

  public StargateTestResource() {}

  public void setIntegrationTestContext(DevServicesContext context) {
    this.containerNetworkId = context.containerNetworkId();
  }

  public void init(Map<String, String> initArgs) {
    this.initArgs = initArgs;
  }

  public Map<String, String> start() {
    if (this.shouldSkip()) {
      return Collections.emptyMap();
    } else {
      boolean reuse = false;
      ImmutableMap.Builder propsBuilder;
      if (this.containerNetworkId.isPresent()) {
        String networkId = (String) this.containerNetworkId.get();
        propsBuilder = this.startWithContainerNetwork(networkId, reuse);
      } else {
        propsBuilder = this.startWithoutContainerNetwork(reuse);
      }

      Integer authPort = this.stargateContainer.getMappedPort(8081);
      String token = this.getAuthToken(this.stargateContainer.getHost(), authPort);
      LOG.info("Using auth token %s for integration tests.".formatted(token));
      propsBuilder.put("stargate.int-test.auth-token", token);
      propsBuilder.put("stargate.int-test.cassandra.host", this.cassandraContainer.getHost());
      propsBuilder.put(
          "stargate.int-test.cassandra.cql-port",
          this.cassandraContainer.getMappedPort(9042).toString());
      String cqlPort = this.stargateContainer.getMappedPort(9042).toString();
      propsBuilder.put("stargate.int-test.coordinator.cql-port", cqlPort);
      propsBuilder.put("stargate.int-test.cluster.persistence", getPersistenceModule());
      // Many ITs create more Collections than default Max 5, use more than 50 indexes so:
      propsBuilder.put(
          "stargate.database.limits.max-collections",
          String.valueOf(getMaxCollectionsPerDBOverride()));
      propsBuilder.put(
          "stargate.database.limits.indexes-available-per-database",
          String.valueOf(getIndexesPerDBOverride()));
      propsBuilder.put(
          "stargate.jsonapi.operations.max-count-limit", String.valueOf(getMaxCountLimit()));
      propsBuilder.put(
          "stargate.jsonapi.operations.default-count-page-size",
          String.valueOf(getCountPageSize()));

      ImmutableMap<String, String> props = propsBuilder.build();
      props.forEach(System::setProperty);
      LOG.info("Using props map for the integration tests: %s".formatted(props));
      return props;
    }
  }

  private boolean shouldSkip() {
    return System.getProperty("quarkus.http.test-host") != null;
  }

  public ImmutableMap.Builder<String, String> startWithoutContainerNetwork(boolean reuse) {
    Network network = this.network();
    this.cassandraContainer = this.baseCassandraContainer(reuse);
    this.cassandraContainer.withNetwork(network);
    this.cassandraContainer.start();
    this.stargateContainer = this.baseCoordinatorContainer(reuse);
    this.stargateContainer.withNetwork(network).withEnv("SEED", "cassandra");
    this.stargateContainer.start();
    Integer bridgePort = this.stargateContainer.getMappedPort(8091);
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.put("quarkus.grpc.clients.bridge.port", String.valueOf(bridgePort));
    return propsBuilder;
  }

  private ImmutableMap.Builder<String, String> startWithContainerNetwork(
      String networkId, boolean reuse) {
    this.cassandraContainer = this.baseCassandraContainer(reuse);
    this.cassandraContainer.withNetworkMode(networkId);
    this.cassandraContainer.start();
    String cassandraHost =
        this.cassandraContainer.getCurrentContainerInfo().getConfig().getHostName();
    this.stargateContainer = this.baseCoordinatorContainer(reuse);
    this.stargateContainer
        .withNetworkMode(networkId)
        .withEnv("BIND_TO_LISTEN_ADDRESS", "true")
        .withEnv("SEED", cassandraHost);
    this.stargateContainer.start();
    String stargateHost =
        this.stargateContainer.getCurrentContainerInfo().getConfig().getHostName();
    ImmutableMap.Builder<String, String> propsBuilder = ImmutableMap.builder();
    propsBuilder.put("quarkus.grpc.clients.bridge.host", stargateHost);
    return propsBuilder;
  }

  public void stop() {
    if (null != this.cassandraContainer && !this.cassandraContainer.isShouldBeReused()) {
      this.cassandraContainer.stop();
    }

    if (null != this.stargateContainer && !this.stargateContainer.isShouldBeReused()) {
      this.stargateContainer.stop();
    }
  }

  private GenericContainer<?> baseCassandraContainer(boolean reuse) {
    String image = this.getCassandraImage();
    GenericContainer<?> container =
        (new GenericContainer(image))
            .withEnv("HEAP_NEWSIZE", "512M")
            .withEnv("MAX_HEAP_SIZE", "2048M")
            .withEnv("CASSANDRA_CGROUP_MEMORY_LIMIT", "true")
            .withEnv("CLUSTER_NAME", getClusterName())
            .withEnv("DC", "datacenter1")
            .withEnv("RACK", "rack1")
            .withEnv(
                "JVM_EXTRA_OPTS",
                "-Dcassandra.skip_wait_for_gossip_to_settle=0 -Dcassandra.load_ring_state=false -Dcassandra.initial_token=1")
            .withNetworkAliases(new String[] {"cassandra"})
            .withExposedPorts(new Integer[] {7000, 9042})
            .withLogConsumer(
                (new Slf4jLogConsumer(LoggerFactory.getLogger("cassandra-docker")))
                    .withPrefix("CASSANDRA"))
            .waitingFor(Wait.forLogMessage(".*Created default superuser role.*\\n", 1))
            .withStartupTimeout(this.getCassandraStartupTimeout())
            .withReuse(reuse);
    if (this.isDse()) {
      container.withEnv("DS_LICENSE", "accept");
    }

    return container;
  }

  private GenericContainer<?> baseCoordinatorContainer(boolean reuse) {
    String image = this.getStargateImage();

    String javaOpts =
        String.format(
            "-Xmx1G -D%s=%d",
            "cassandra.sai.max_string_term_size_kb", DEFAULT_SAI_MAX_STRING_TERM_SIZE_KB);

    GenericContainer<?> container =
        (new GenericContainer(image))
            .withEnv("JAVA_OPTS", javaOpts)
            .withEnv("CLUSTER_NAME", getClusterName())
            .withEnv("DATACENTER_NAME", "datacenter1")
            .withEnv("RACK_NAME", "rack1")
            // .withEnv("SIMPLE_SNITCH", "true")
            .withEnv("ENABLE_AUTH", "true")
            .withNetworkAliases(new String[] {"coordinator"})
            .withExposedPorts(new Integer[] {8091, 8081, 8084, 9042})
            .withLogConsumer(
                (new Slf4jLogConsumer(LoggerFactory.getLogger("coordinator-docker")))
                    .withPrefix("COORDINATOR"))
            .waitingFor(Wait.forHttp("/checker/readiness").forPort(8084).forStatusCode(200))
            .withStartupTimeout(this.getCoordinatorStartupTimeout())
            .withReuse(reuse);
    // if (this.isDse()) {
    //   container.withEnv("DSE", "1");
    // }

    return container;
  }

  private Network network() {
    if (null == this.network) {
      this.network = Network.newNetwork();
    }

    return this.network;
  }

  private String getCassandraImage() {
    String image = System.getProperty("testing.containers.cassandra-image");
    return null == image ? "cassandra:4.0.10" : image;
  }

  private String getStargateImage() {
    String image = System.getProperty("testing.containers.stargate-image");
    return null == image ? "stargateio/coordinator-4_0:v2.1" : image;
  }

  private static String getClusterName() {
    return System.getProperty("testing.containers.cluster-name", "int-test-cluster");
  }

  public static String getPersistenceModule() {
    return System.getProperty(
        "testing.containers.cluster-persistence", "persistence-cassandra-4.0");
  }

  private boolean isDse() {
    String dse =
        System.getProperty(
            "testing.containers.cluster-dse", StargateTestResource.Defaults.CLUSTER_DSE);
    return "true".equals(dse);
  }

  private Duration getCassandraStartupTimeout() {
    long cassandraStartupTimeout = Long.getLong("testing.containers.cassandra-startup-timeout", 2L);
    return Duration.ofMinutes(cassandraStartupTimeout);
  }

  private Duration getCoordinatorStartupTimeout() {
    long coordinatorStartupTimeout =
        Long.getLong("testing.containers.coordinator-startup-timeout", 3L);
    return Duration.ofMinutes(coordinatorStartupTimeout);
  }

  private String getAuthToken(String host, int authPort) {
    try {
      String json = "{\n  \"username\":\"cassandra\",\n  \"password\":\"cassandra\"\n}\n";
      URI authUri = new URI("http://%s:%d/v1/auth".formatted(host, authPort));
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(authUri)
              .header("Content-Type", "application/json")
              .POST(BodyPublishers.ofString(json))
              .build();
      HttpResponse<String> response =
          HttpClient.newHttpClient().send(request, BodyHandlers.ofString());
      ObjectMapper objectMapper = new ObjectMapper();
      AuthResponse authResponse =
          (AuthResponse) objectMapper.readValue((String) response.body(), AuthResponse.class);
      return authResponse.authToken;
    } catch (Exception var9) {
      throw new RuntimeException("Failed to get Cassandra token for integration tests.", var9);
    }
  }

  public abstract int getMaxCollectionsPerDBOverride();

  public abstract int getIndexesPerDBOverride();

  public abstract int getMaxCountLimit();

  public abstract int getCountPageSize();

  interface Defaults {
    String CASSANDRA_IMAGE = "cassandra";
    String CASSANDRA_IMAGE_TAG = "4.0.10";
    String STARGATE_IMAGE = "stargateio/coordinator-4_0";
    String STARGATE_IMAGE_TAG = "v2.1";
    String CLUSTER_NAME = "int-test-cluster";
    String PERSISTENCE_MODULE = "persistence-cassandra-4.0";
    String CLUSTER_DSE = "true";
    long CASSANDRA_STARTUP_TIMEOUT = 2L;
    long COORDINATOR_STARTUP_TIMEOUT = 3L;
  }

  static record AuthResponse(String authToken) {
    AuthResponse(String authToken) {
      this.authToken = authToken;
    }

    public String authToken() {
      return this.authToken;
    }
  }
}
