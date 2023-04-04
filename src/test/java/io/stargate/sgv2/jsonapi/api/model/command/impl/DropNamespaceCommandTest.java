package io.stargate.sgv2.jsonapi.api.model.command.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import java.util.Set;
import javax.inject.Inject;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class DropNamespaceCommandTest {

  @Inject ObjectMapper objectMapper;

  @Inject Validator validator;

  @Nested
  class Validation {

    @Test
    public void noName() throws Exception {
      String json =
          """
          {
            "dropNamespace": {
            }
          }
          """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must not be null");
    }

    @Test
    public void nameTooLong() throws Exception {
      String json =
          """
          {
            "dropNamespace": {
              "name": "%s"
            }
          }
          """
              .formatted(RandomStringUtils.randomAlphabetic(49));

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("size must be between 1 and 48");
    }

    @Test
    public void nameWrongPattern() throws Exception {
      String json =
          """
          {
            "dropNamespace": {
              "name": "_not_possible"
            }
          }
          """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must match \"[a-zA-Z][a-zA-Z0-9_]*\"");
    }

    @Test
    public void systemKeyspaceNotAllowed() throws Exception {
      String json =
          """
          {
            "dropNamespace": {
              "name": "system"
            }
          }
          """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

      assertThat(result)
          .isNotEmpty()
          .extracting(ConstraintViolation::getMessage)
          .contains("must match \"^(?!system$).*\"");
    }

    @Test
    public void nameCorrectPattern() throws Exception {
      String json =
          """
          {
            "dropNamespace": {
              "name": "my_system"
            }
          }
          """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }

    @Test
    public void nameCorrectPatternSystemPrefix() throws Exception {
      String json =
          """
          {
            "dropNamespace": {
              "name": "system_keyspace"
            }
          }
          """;

      DropNamespaceCommand command = objectMapper.readValue(json, DropNamespaceCommand.class);
      Set<ConstraintViolation<DropNamespaceCommand>> result = validator.validate(command);

      assertThat(result).isEmpty();
    }
  }
}
