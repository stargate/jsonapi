package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import static io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache.OFFLINE_WRITER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.api.model.command.impl.*;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.CQLSessionCache;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.embedding.operation.EmbeddingService;
import io.stargate.sgv2.jsonapi.service.processor.CommandProcessor;
import io.stargate.sgv2.jsonapi.service.resolver.CommandResolverService;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.BeginOfflineSessionCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.EndOfflineSessionCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineGetStatusCommandResolver;
import io.stargate.sgv2.jsonapi.service.resolver.model.impl.OfflineInsertManyCommandResolver;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutablePair;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.Pair;

public class OfflineLoaderTester {
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    OfflineFileWriterInitializer.initialize();
    int iterationCount = 1;
    int threadPoolSize = 1;
    ExecutorService executeService = Executors.newFixedThreadPool(threadPoolSize);
    for (int i = 0; i < iterationCount; i++) {
      // System.out.println("Starting thread " + i);
      int finalI = i;
      executeService.submit(
          () -> {
            OfflineLoaderTester offlineLoaderTester = new OfflineLoaderTester();
            try {
              List<String> sessions = offlineLoaderTester.testLoadData();
              System.out.println("Thread " + finalI + " Sessions : " + sessions);
            } catch (Exception e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }
          });
    }
    executeService.shutdown();
    while (!executeService.isTerminated()) {
      Thread.sleep(1000);
      System.out.println("Waiting for threads to complete");
    }
    System.out.println("All threads completed");
  }

  private List<String> testLoadData()
      throws JsonProcessingException, ExecutionException, InterruptedException {
    List<String> sessionIds = new ArrayList<>();
    // System.out.println(threadId + "Started");
    boolean vectorTest = true;
    int totalRecords = 100_000;
    int chuckSize = totalRecords / 20;
    int createNewSessionAfterDataInMB = 20;
    List<JsonNode> records = getRecords(vectorTest, totalRecords);
    List<List<JsonNode>> recordsList = new ArrayList<>(Lists.partition(records, chuckSize));
    boolean newSession = true;
    String sessionId = null;
    boolean closeSession = true;
    CommandContext commandContext = null;
    for (List<JsonNode> recordList : recordsList) {
      if (newSession) {
        Pair<String, CommandContext> beginSession = OfflineLoaderTester.beginSession(vectorTest);
        sessionId = beginSession.getLeft();
        sessionIds.add(sessionId);
        commandContext = beginSession.getRight();
        closeSession = true;
      }
      OfflineLoaderTester.loadData(sessionId, commandContext, recordList);
      CommandResult statusResult = OfflineLoaderTester.getStatus(commandContext, sessionId);
      if (canEndSession(statusResult, createNewSessionAfterDataInMB)) {
        closeSession = false;
        OfflineLoaderTester.endSession(sessionId, commandContext);
        newSession = true;
      } else {
        newSession = false;
      }
    }
    if (closeSession) {
      OfflineLoaderTester.endSession(sessionId, commandContext);
    }
    System.out.println("Thread " + Thread.currentThread().getId() + " Done!");
    return sessionIds;
  }

  private static boolean canEndSession(
      CommandResult statusResult, int createNewSessionAfterDataInMB) {
    OfflineWriterSessionStatus offlineWriterSessionStatus =
        (OfflineWriterSessionStatus)
            statusResult.status().get(CommandStatus.OFFLINE_WRITER_SESSION_STATUS);
    return offlineWriterSessionStatus.dataDirectorySizeInMB() >= createNewSessionAfterDataInMB;
  }

  private static Pair<String, CommandContext> beginSession(boolean vectorTest)
      throws ExecutionException, InterruptedException {
    CommandProcessor commandProcessor =
        new CommandProcessor(buildQueryExecutor(), buildCommandResolverService());
    // Create the beginOfflineSession command so we can create a new offline session
    String namespace = "demo_namespace";
    String collection = "players";
    String ssTablesOutputDirectory = "/var/tmp/sstables_test";
    int vectorSize = 3;
    int fileWriterBufferSizeInMB = 5;
    String similarityFunction = CollectionSettings.SimilarityFunction.COSINE.toString();

    var vectorSearchConfig =
        new CreateCollectionCommand.Options.VectorSearchConfig(vectorSize, similarityFunction);
    var options =
        new CreateCollectionCommand.Options(vectorTest ? vectorSearchConfig : null, null, null);
    var createCollectionCommand = new CreateCollectionCommand(collection, options);
    var beginOfflineSessionCommand =
        new BeginOfflineSessionCommand(
            namespace, createCollectionCommand, ssTablesOutputDirectory, fileWriterBufferSizeInMB);

    EmbeddingService embeddingService = null; // TODO
    JsonProcessingMetricsReporter jsonProcessingMetricsReporter = null; // TODO

    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo();

    CommandContext commandContext =
        CommandContext.from(
            namespace,
            collection,
            beginOfflineSessionCommand.getCollectionSettings(),
            embeddingService,
            beginOfflineSessionCommand.getClass().getSimpleName());
    CommandResult commandResult =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, beginOfflineSessionCommand)
            .onFailure()
            .invoke(t -> System.out.println("Error : " + t))
            .subscribe()
            .asCompletionStage()
            .get();
    // TODO - SL - ? Can we make a OfflineBeginSessionResponse.fromCommandResult(commandResult) ?
    // so we can hide the CommandResult from the user and give them a typed result that understands
    // the command
    String sessionId =
        commandResult.status().get(CommandStatus.OFFLINE_WRITER_SESSION_ID).toString();
    return new ImmutablePair<>(sessionId, commandContext);
  }

  private static CommandResult loadData(
      String sessionId, CommandContext commandContext, List<JsonNode> records)
      throws ExecutionException, InterruptedException, JsonProcessingException {
    CommandProcessor commandProcessor =
        new CommandProcessor(buildQueryExecutor(), buildCommandResolverService());
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo();
    dataApiRequestInfo.setTenantId(sessionId);
    // TODO - SL, what is the max number of docs in the offline insert many ?
    // TODO - SL, what happens if some documents fail ?
    OfflineInsertManyCommand offlineInsertManyCommand =
        new OfflineInsertManyCommand(sessionId, records);
    CommandResult offlineInsertManyCommandResponse =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineInsertManyCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    List<DocumentId> insertedIds =
        (List<DocumentId>)
            offlineInsertManyCommandResponse.status().get(CommandStatus.INSERTED_IDS);
    System.out.println("Loaded data up to: " + insertedIds.get(insertedIds.size() - 1));
    return offlineInsertManyCommandResponse;
  }

  public static CommandResult endSession(String sessionId, CommandContext commandContext)
      throws ExecutionException, InterruptedException {
    CommandProcessor commandProcessor =
        new CommandProcessor(buildQueryExecutor(), buildCommandResolverService());
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo();
    dataApiRequestInfo.setTenantId(sessionId);
    // TODO SL - response should include file path and size
    EndOfflineSessionCommand offlineEndWriterCommand = new EndOfflineSessionCommand(sessionId);
    CommandResult offlineEndWriterCommandResponse =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineEndWriterCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    System.out.println("Ended session : " + offlineEndWriterCommandResponse);
    return offlineEndWriterCommandResponse;
  }

  public static CommandResult getStatus(CommandContext commandContext, String sessionId)
      throws ExecutionException, InterruptedException {
    CommandProcessor commandProcessor =
        new CommandProcessor(buildQueryExecutor(), buildCommandResolverService());
    DataApiRequestInfo dataApiRequestInfo = new DataApiRequestInfo();
    dataApiRequestInfo.setTenantId(sessionId);
    // TODO: SL - Add a test that loops calling insertMany, checking the size of the SSTable with
    // get status and stops when it gets to 10 mb ?
    // TODO: AL - a test that is running multiple sessions at the same time

    // TODO SL - remember to include the file size
    OfflineGetStatusCommand offlineGetStatusCommand = new OfflineGetStatusCommand(sessionId);
    CommandResult offlineGetStatusCommandResponse =
        commandProcessor
            .processCommand(dataApiRequestInfo, commandContext, offlineGetStatusCommand)
            .subscribe()
            .asCompletionStage()
            .get();
    System.out.println("Status : " + offlineGetStatusCommandResponse);

    // System.out.println(offlineGetStatusCommandResponse);
    /*System.out.println("Statements : ");
    System.out.println(beginOfflineSessionCommand.getFileWriterParams().createTableCQL() + ";");
    beginOfflineSessionCommand
        .getFileWriterParams()
        .indexCQLs()
        .forEach(s -> System.out.println(s + ";"));
    System.out.println(beginOfflineSessionCommand.getFileWriterParams().insertStatementCQL() + ";");*/
    return offlineGetStatusCommandResponse;
  }

  private static List<JsonNode> getRecords(boolean isVectorEnabled, int totalRecords)
      throws JsonProcessingException {
    List<JsonNode> records = new ArrayList<>();
    String template = """
        {"_id": %s,"name": "person%s", "age":"%s"%s}
        """;
    for (int i = 0; i < totalRecords; i++) {
      records.add(
          new ObjectMapper()
              .readTree(
                  String.format(
                      template,
                      i,
                      i,
                      (int) (Math.random() * 99) + 1,
                      isVectorEnabled ? ",\"$vector\": [0.3,0.4,0.5]" : "")));
    }
    return records;
  }

  private static CommandResolverService buildCommandResolverService() {
    ObjectMapper objectMapper = new ObjectMapper();
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder().withMapping(DocumentLimitsConfig.class).build();
    DocumentLimitsConfig documentLimitsConfig =
        smallRyeConfig.getConfigMapping(DocumentLimitsConfig.class);
    Shredder shredder = new Shredder(objectMapper, documentLimitsConfig, null);
    return new CommandResolverService(
        List.of(
            new BeginOfflineSessionCommandResolver(shredder, objectMapper),
            new EndOfflineSessionCommandResolver(shredder, objectMapper),
            new OfflineGetStatusCommandResolver(shredder, objectMapper),
            new OfflineInsertManyCommandResolver(shredder, objectMapper)));
  }

  private static QueryExecutor buildQueryExecutor() {
    SmallRyeConfig smallRyeConfig =
        new SmallRyeConfigBuilder()
            .withMapping(OperationsConfig.class)
            // TODO-SL increase cache expiry limit
            .withDefaultValue("stargate.jsonapi.operations.database-config.type", OFFLINE_WRITER)
            .build();
    OperationsConfig operationsConfig = smallRyeConfig.getConfigMapping(OperationsConfig.class);
    CQLSessionCache cqlSessionCache =
        new CQLSessionCache(operationsConfig, new SimpleMeterRegistry());
    return new QueryExecutor(cqlSessionCache, operationsConfig);
  }
}
