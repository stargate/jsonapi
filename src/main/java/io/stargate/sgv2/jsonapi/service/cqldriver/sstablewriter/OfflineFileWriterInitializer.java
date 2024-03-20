package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import io.smallrye.context.SmallRyeContextManagerProvider;

public class OfflineFileWriterInitializer {
  private static final String OFFLINE_WRITER_MODE_PROPERTY = "stargate.sstablewriter.offline";

  public static void initialize() {
    System.setProperty(OFFLINE_WRITER_MODE_PROPERTY, "true");
    SmallRyeContextManagerProvider.instance();
  }

  public static boolean isOffline() {
    return Boolean.parseBoolean(System.getProperty(OFFLINE_WRITER_MODE_PROPERTY, "false"));
  }
}
