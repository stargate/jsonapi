package io.stargate.sgv2.jsonapi.service.processor;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommands;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class MeteredCommandProcessorTest {
  @Inject MeteredCommandProcessor meteredCommandProcessor;
  @InjectMock protected CommandProcessor commandProcessor;
  @InjectMock protected StargateRequestInfo stargateRequestInfo;
  @Inject ObjectMapper objectMapper;

  @Nested
  class CustomMetrics {
    @Test
    public void metrics() throws Exception {
      String json =
          """
          {
            "countDocuments": {

            }
          }
          """;

      CountDocumentsCommands countCommand =
          objectMapper.readValue(json, CountDocumentsCommands.class);
      CommandContext commandContext = new CommandContext("namespace", "collection");
      CommandResult commandResult = new CommandResult(Collections.emptyList());
      Mockito.when(commandProcessor.processCommand(commandContext, countCommand))
          .thenReturn(Uni.createFrom().item(commandResult));
      Mockito.when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of("test-tenant"));
      meteredCommandProcessor.processCommand(commandContext, countCommand).await().indefinitely();
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> httpMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("command_processor_process")
                          && line.contains("error=\"false\""))
              .toList();

      assertThat(httpMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"CountDocumentsCommands\"");
                      assertThat(line).contains("tenant=\"test-tenant\"");
                      assertThat(line).contains("error=\"false\"");
                      assertThat(line).contains("error_code=\"NA\"");
                      assertThat(line).contains("error_class=\"NA\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                    });
              });
    }

    @Test
    public void errorMetrics() throws Exception {
      String json =
          """
                                    {
                                      "countDocuments": {

                                      }
                                    }
                                    """;

      CountDocumentsCommands countCommand =
          objectMapper.readValue(json, CountDocumentsCommands.class);
      CommandContext commandContext = new CommandContext("namespace", "collection");
      Map<String, Object> fields = new HashMap<>();
      fields.put("exceptionClass", "TestExceptionClass");
      fields.put("errorCode", "TestErrorCode");
      CommandResult.Error error = new CommandResult.Error("message", fields, Response.Status.OK);
      CommandResult commandResult = new CommandResult(Collections.singletonList(error));
      Mockito.when(commandProcessor.processCommand(commandContext, countCommand))
          .thenReturn(Uni.createFrom().item(commandResult));
      Mockito.when(stargateRequestInfo.getTenantId()).thenReturn(Optional.of("test-tenant"));
      meteredCommandProcessor.processCommand(commandContext, countCommand).await().indefinitely();
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> httpMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("command_processor_process")
                          && line.contains("error=\"true\""))
              .toList();

      assertThat(httpMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"CountDocumentsCommands\"");
                      assertThat(line).contains("tenant=\"test-tenant\"");
                      assertThat(line).contains("error=\"true\"");
                      assertThat(line).contains("error_code=\"TestErrorCode\"");
                      assertThat(line).contains("error_class=\"TestExceptionClass\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                    });
              });
    }
  }
}
