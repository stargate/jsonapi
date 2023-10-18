package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.CqlSession;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CQLSessionHandler {
  private static final Logger logger = LoggerFactory.getLogger(CQLSessionHandler.class);

  private static CqlSession session;
  private static boolean isInitialed;

  protected static CqlSession getSession() {
    if (isInitialed) {
      return session;
    } else {
      init();
      return session;
    }
  }

  private static synchronized void init() {
    if (isInitialed) {
      return;
    }
    if (Boolean.getBoolean("MOCK_BRIDGE") || System.getenv("MOCK_BRIDGE") != null) {
      try {

        // Dev 5 testing
        //      var token =
        // "AstraCS:zZehRZfHFfykFmDCzipEnJMZ:169baaa8bc7d3c8c875aeaa913289eb28b0c283ecc5792c1274cd6ea0019bfb0";
        //      var contactHost =
        // "cndb-coordinators.3f5e34ca-99e6-4d06-b7a2-08131921a1c7.svc.cluster.local";
        //      var port = 9042;
        //      var keyspace =
        // "33663565333463612d393965362d346430362d623761322d303831333139323161316337_baselines";
        //      var table = "keyvalue";
        // var user = "token"

        boolean isLocal = System.getenv("IS_LOCAL") != null;
        /*var token =
            "AstraCS:zZehRZfHFfykFmDCzipEnJMZ:169baaa8bc7d3c8c875aeaa913289eb28b0c283ecc5792c1274cd6ea0019bfb0";
        var contactHost =
            "cndb-coordinators.3f5e34ca-99e6-4d06-b7a2-08131921a1c7.svc.cluster.local";
        var port = 9042;
        var keyspace =
            "33663565333463612d393965362d346430362d623761322d303831333139323161316337_baselines";
        var user = "token";*/
        /*if (isLocal) {
          // local testing
          var token = "cassandra";
          var contactHost = "127.0.0.1";
          var port = 9042;
          var keyspace = "perf_test";
          var user = "cassandra";
          session =
              CqlSession.builder()
                  .withAuthCredentials(user, token)
                  .addContactPoint(new InetSocketAddress(contactHost, port))
                  .withKeyspace(keyspace)
                  .build();
        } else {*/
        session =
            CqlSession.builder()
                .withCloudSecureConnectBundle(
                    Paths.get(
                        CQLSessionHandler.class.getClassLoader().getResource("cql.zip").toURI()))
                .withAuthCredentials(
                    "token",
                    "AstraCS:zZehRZfHFfykFmDCzipEnJMZ:169baaa8bc7d3c8c875aeaa913289eb28b0c283ecc5792c1274cd6ea0019bfb0")
                .withKeyspace("baselines")
                .build();
        // }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    isInitialed = true;
  }
}
