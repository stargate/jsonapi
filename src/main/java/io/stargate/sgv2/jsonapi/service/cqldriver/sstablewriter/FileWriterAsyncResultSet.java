package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class FileWriterAsyncResultSet implements AsyncResultSet {

  private final ColumnDefinitions columnDefs;
  private final Row insertResponseRow;

  public FileWriterAsyncResultSet(ColumnDefinitions columnDefs, Row insertResponseRow) {
    this.columnDefs = columnDefs;
    this.insertResponseRow = insertResponseRow;
  }

  @NonNull
  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return this.columnDefs;
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int remaining() {
    throw new UnsupportedOperationException();
  }

  @NonNull
  @Override
  public Iterable<Row> currentPage() {
    return List.of(insertResponseRow);
  }

  @Override
  public boolean hasMorePages() {
    return false;
  }

  @NonNull
  @Override
  public CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException {
    return null;
  }

  @Override
  public boolean wasApplied() {
    return insertResponseRow.getBoolean("[applied]");
  }
}
