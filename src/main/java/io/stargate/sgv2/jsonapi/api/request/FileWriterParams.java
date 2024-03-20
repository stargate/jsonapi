package io.stargate.sgv2.jsonapi.api.request;

import java.util.List;

public record FileWriterParams(
    String keyspaceName,
    String tableName,
    String ssTableOutputDirectory,
    int fileWriterBufferSizeInMB,
    String createTableCQL,
    String insertStatementCQL,
    List<String> indexCQLs,
    boolean vectorEnabled) {
  public FileWriterParams {
    if (keyspaceName == null
        || tableName == null
        || ssTableOutputDirectory == null
        || createTableCQL == null
        || insertStatementCQL == null) {
      throw new IllegalArgumentException("All parameters must be non-null");
    }
  }
}
