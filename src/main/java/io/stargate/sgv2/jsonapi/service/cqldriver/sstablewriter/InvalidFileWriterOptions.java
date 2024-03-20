package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

public class InvalidFileWriterOptions extends RuntimeException {
  public InvalidFileWriterOptions(String message) {
    super(message);
  }
}
