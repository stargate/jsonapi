package io.stargate.sgv2.jsonapi.api.model.command.clause.update;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.util.regex.Pattern;

/** Factory for {@link UpdateTarget} instances. */
public class UpdateTargetLocator {
  private static final Pattern DOT = Pattern.compile(Pattern.quote("."));

  private static final Pattern INDEX_SEGMENT = Pattern.compile("0|[1-9][0-9]*");

  /**
   * Method that will create target instance for given path through given document; if no such path
   * exists, will not attempt to create path (nor report any problems).
   *
   * <p>Used for $unset operation.
   *
   * @param document Document that may contain target path
   * @param dotPath Path that points to possibly existing target
   * @return Target instance with optional context and value nodes
   */
  public UpdateTarget findIfExists(JsonNode document, String dotPath) {
    String[] segments = splitAndVerify(dotPath);
    JsonNode context = document;
    final int lastSegmentIndex = segments.length - 1;

    // First traverse all but the last segment
    for (int i = 0; i < lastSegmentIndex; ++i) {
      final String segment = segments[i];
      // Simple logic: Object nodes traversed via property; Arrays index; others can't
      if (context.isObject()) {
        context = context.get(segment);
      } else if (context.isArray()) {
        int index = findIndexFromSegment(segment);
        // Arrays MUST be accessed via index but here mismatch will not result
        // in exception (as having path is optional).
        context = (index < 0) ? null : context.get(index);
      } else {
        context = null;
      }
      if (context == null) {
        return UpdateTarget.missingPath(dotPath);
      }
    }

    // But the last segment is special since we now may get Value node but also need
    // to denote how context refers to it (Object property vs Array index)
    final String segment = segments[lastSegmentIndex];
    if (context.isObject()) {
      return UpdateTarget.pathViaObject(dotPath, context, context.get(segment), segment);
    } else if (context.isArray()) {
      int index = findIndexFromSegment(segment);
      if (index < 0) {
        return UpdateTarget.missingPath(dotPath);
      }
      return UpdateTarget.pathViaArray(dotPath, context, context.get(index), index);
    } else {
      return UpdateTarget.missingPath(dotPath);
    }
  }

  /**
   * Method that will create target instance for given path through given document; if no such path
   * exists, will try to create path. Creation may fail with an exception for cases like path trying
   * to create properties on Array nodes.
   *
   * <p>Used for update operations that add or modify values (operations other than $unset)
   *
   * @param document Document that is to contain target path
   * @param dotPath Path that points to target that may exists, or is about to be added
   * @return Target instance with non-null context node, optional value node
   */
  public UpdateTarget findOrCreate(JsonNode document, String dotPath) {
    String[] segments = splitAndVerify(dotPath);
    JsonNode context = document;
    final int lastSegmentIndex = segments.length - 1;

    // First traverse all but the last segment
    for (int i = 0; i < lastSegmentIndex; ++i) {
      final String segment = segments[i];
      JsonNode nextContext;

      // Simple logic: Object nodes traversed via property; Arrays index; others can't
      if (context.isObject()) {
        nextContext = context.get(segment);
        if (nextContext == null) {
          nextContext = ((ObjectNode) context).putObject(segment);
        }
      } else if (context.isArray()) {
        int index = findIndexFromSegment(segment);
        // Arrays MUST be accessed via index but here mismatch will not result
        // in exception (as having path is optional).
        if (index < 0) {
          throw cantCreatePropertyPath(dotPath, segment, context);
        }
        // Ok; either existing path (within existing array)
        ArrayNode array = (ArrayNode) context;
        nextContext = context.get(index);
        // Or, if not within, then need to create, including null padding
        if (nextContext == null) {
          // Fill up padding up to -- but NOT INCLUDING -- position to add
          while (array.size() < index) {
            array.addNull();
          }
          // Also: must assume Object to add, no way to induce "missing" Arrays
          nextContext = ((ArrayNode) context).addObject();
        }
      } else {
        throw cantCreatePropertyPath(dotPath, segment, context);
      }
      context = nextContext;
    }

    // But the last segment is special since we now may get Value node but also need
    // to denote how context refers to it (Object property vs Array index)
    final String segment = segments[lastSegmentIndex];
    if (context.isObject()) {
      return UpdateTarget.pathViaObject(dotPath, context, context.get(segment), segment);
    }
    if (context.isArray()) {
      int index = findIndexFromSegment(segment);
      // Cannot create properties on Arrays
      if (index < 0) {
        throw cantCreatePropertyPath(dotPath, segment, context);
      }
      return UpdateTarget.pathViaArray(dotPath, context, context.get(index), index);
    }
    // Cannot create properties on Atomics either
    throw cantCreatePropertyPath(dotPath, segment, context);
  }

  private String[] splitAndVerify(String dotPath) {
    String[] result = DOT.split(dotPath);
    for (String segment : result) {
      if (segment.isEmpty()) {
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH,
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage()
                + ": empty segment ('') in path '"
                + dotPath
                + "'");
      }
    }
    return result;
  }

  private int findIndexFromSegment(String segment) {
    if (INDEX_SEGMENT.matcher(segment).matches()) {
      return Integer.parseInt(segment);
    }
    return -1;
  }

  private JsonApiException cantCreatePropertyPath(String fullPath, String prop, JsonNode context) {
    return new JsonApiException(
        ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH,
        String.format(
            "%s: cannot create field ('%s') in path '%s'; only OBJECT nodes have properties (got %s)",
            ErrorCode.UNSUPPORTED_UPDATE_OPERATION_PATH.getMessage(),
            prop,
            fullPath,
            context.getNodeType()));
  }
}
