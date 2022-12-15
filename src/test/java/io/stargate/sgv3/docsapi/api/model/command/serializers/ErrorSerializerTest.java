package io.stargate.sgv3.docsapi.api.model.command.serializers;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv3.docsapi.api.model.command.CommandResult;
import java.util.Map;
import javax.inject.Inject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
class ErrorSerializerTest {

  @Inject ObjectMapper objectMapper;

  @Nested
  class Serialize {

    @Test
    public void happyPath() throws Exception {
      CommandResult.Error error = new CommandResult.Error("My message.", Map.of("field", "value"));

      String result = objectMapper.writeValueAsString(error);

      assertThat(result)
          .isEqualTo("""
                    {"message":"My message.","field":"value"}""");
    }

    @Test
    public void withoutProps() throws Exception {
      CommandResult.Error error = new CommandResult.Error("My message.");

      String result = objectMapper.writeValueAsString(error);

      assertThat(result).isEqualTo("""
                    {"message":"My message."}""");
    }

    @Test
    public void messageFieldIgnored() throws Exception {
      CommandResult.Error error =
          new CommandResult.Error("My message.", Map.of("message", "value"));

      String result = objectMapper.writeValueAsString(error);

      assertThat(result).isEqualTo("""
                    {"message":"My message."}""");
    }

    @Test
    public void withNulls() throws Exception {
      CommandResult.Error error = new CommandResult.Error(null, null);

      String result = objectMapper.writeValueAsString(error);

      assertThat(result).isEqualTo("""
                    {"message":null}""");
    }
  }
}
