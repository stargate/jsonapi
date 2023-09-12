package io.stargate.sgv2.jsonapi.service.embedding.operation;

import dev.ai4j.openai4j.OpenAiClient;
import dev.ai4j.openai4j.embedding.EmbeddingRequest;
import dev.ai4j.openai4j.embedding.EmbeddingResponse;
import java.util.List;
import java.util.stream.Collectors;

public class OpenAiEmbeddingClient implements EmbeddingService {
  private String modelName;
  OpenAiClient client;

  public OpenAiEmbeddingClient(String baseUrl, String apiKey, String modelName) {
    this.modelName = modelName;
    client = OpenAiClient.builder().openAiApiKey(apiKey).baseUrl(baseUrl).build();
  }

  public OpenAiEmbeddingClient(String apiKey, String modelName) {
    this("https://api.openai.com/v1/", apiKey, modelName);
  }

  public List<float[]> vectorize(List<String> texts) {
    EmbeddingRequest request = EmbeddingRequest.builder().input(texts).model(modelName).build();
    EmbeddingResponse response = execute(request);
    return response.data().stream()
        .map(openAiEmbedding -> openAiEmbedding.embedding())
        .map(
            embedding -> {
              float[] vector = new float[embedding.size()];
              for (int i = 0; i < embedding.size(); i++) {
                vector[i] = embedding.get(i).floatValue();
              }
              return vector;
            })
        .collect(Collectors.toList());
  }

  private EmbeddingResponse execute(EmbeddingRequest request) {
    return client.embedding(request).execute();
  }
}
