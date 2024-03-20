package io.stargate.sgv2.jsonapi.service.processor;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CountDocumentsCommand;
import io.stargate.sgv2.jsonapi.api.model.command.impl.FindCommand;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class MeteredCommandProcessorTest {
  @Inject MeteredCommandProcessor meteredCommandProcessor;
  @InjectMock protected CommandProcessor commandProcessor;
  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;
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

      CountDocumentsCommand countCommand =
          objectMapper.readValue(json, CountDocumentsCommand.class);
      CommandContext commandContext = new CommandContext("namespace", "collection");
      CommandResult commandResult = new CommandResult(Collections.emptyList());
      Mockito.when(
              commandProcessor.processCommand(dataApiRequestInfo, commandContext, countCommand))
          .thenReturn(Uni.createFrom().item(commandResult));
      Mockito.when(dataApiRequestInfo.getTenantId()).thenReturn(Optional.of("test-tenant"));
      meteredCommandProcessor
          .processCommand(dataApiRequestInfo, commandContext, countCommand)
          .await()
          .atMost(Duration.ofMinutes(1));
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> httpMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("command_processor_process")
                          && !line.startsWith("command_processor_process_seconds_bucket")
                          && !line.contains("quantile")
                          && line.contains("error=\"false\""))
              .toList();

      assertThat(httpMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"CountDocumentsCommand\"");
                      assertThat(line).contains("tenant=\"test-tenant\"");
                      assertThat(line).contains("error=\"false\"");
                      assertThat(line).contains("error_code=\"NA\"");
                      assertThat(line).contains("error_class=\"NA\"");
                      assertThat(line).contains("module=\"sgv2-jsonapi\"");
                    });
              });
    }

    @Test
    public void errorMetricsWithNoErrorCode() throws Exception {
      String json = """
        {
          "find": {

          }
        }
        """;

      FindCommand countCommand = objectMapper.readValue(json, FindCommand.class);
      CommandContext commandContext = new CommandContext("namespace", "collection");
      Map<String, Object> fields = new HashMap<>();
      fields.put("exceptionClass", "TestExceptionClass");
      CommandResult.Error error =
          new CommandResult.Error("message", fields, fields, Response.Status.OK);
      CommandResult commandResult = new CommandResult(Collections.singletonList(error));
      Mockito.when(
              commandProcessor.processCommand(dataApiRequestInfo, commandContext, countCommand))
          .thenReturn(Uni.createFrom().item(commandResult));
      Mockito.when(dataApiRequestInfo.getTenantId()).thenReturn(Optional.of("test-tenant"));
      meteredCommandProcessor
          .processCommand(dataApiRequestInfo, commandContext, countCommand)
          .await()
          .atMost(Duration.ofMinutes(1));
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> httpMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("command_processor_process_seconds_")
                          && !line.contains("seconds_bucket")
                          && line.contains("error=\"true\"")
                          && !line.startsWith("command_processor_process_seconds_bucket")
                          && !line.contains("quantile")
                          && line.contains("command=\"FindCommand\""))
              .toList();

      assertThat(httpMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"FindCommand\"");
                      assertThat(line).contains("tenant=\"test-tenant\"");
                      assertThat(line).contains("error=\"true\"");
                      assertThat(line).contains("error_code=\"unknown\"");
                      assertThat(line).contains("error_class=\"TestExceptionClass\"");
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

      CountDocumentsCommand countCommand =
          objectMapper.readValue(json, CountDocumentsCommand.class);
      CommandContext commandContext = new CommandContext("namespace", "collection");
      Map<String, Object> fields = new HashMap<>();
      fields.put("exceptionClass", "TestExceptionClass");
      fields.put("errorCode", "TestErrorCode");
      CommandResult.Error error =
          new CommandResult.Error("message", fields, fields, Response.Status.OK);
      CommandResult commandResult = new CommandResult(Collections.singletonList(error));
      Mockito.when(
              commandProcessor.processCommand(dataApiRequestInfo, commandContext, countCommand))
          .thenReturn(Uni.createFrom().item(commandResult));
      Mockito.when(dataApiRequestInfo.getTenantId()).thenReturn(Optional.of("test-tenant"));
      meteredCommandProcessor
          .processCommand(dataApiRequestInfo, commandContext, countCommand)
          .await()
          .atMost(Duration.ofMinutes(1));
      String metrics = given().when().get("/metrics").then().statusCode(200).extract().asString();
      List<String> httpMetrics =
          metrics
              .lines()
              .filter(
                  line ->
                      line.startsWith("command_processor_process_")
                          && line.contains("error=\"true\"")
                          && !line.startsWith("command_processor_process_seconds_bucket")
                          && !line.contains("quantile")
                          && line.contains("command=\"CountDocumentsCommand\""))
              .toList();

      assertThat(httpMetrics)
          .satisfies(
              lines -> {
                assertThat(lines.size()).isEqualTo(3);
                lines.forEach(
                    line -> {
                      assertThat(line).contains("command=\"CountDocumentsCommand\"");
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
