package io.stargate.sgv2.jsonapi.service.cqldriver;

public class AlreadyInitializedException extends RuntimeException {
  public AlreadyInitializedException(String message) {
    super(message);
  }
}
