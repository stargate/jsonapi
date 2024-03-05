package io.stargate.sgv2.jsonapi.service.embedding.operation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.quarkus.rest.client.reactive.QuarkusRestClientBuilder;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * Interface that accepts a list of texts that needs to be vectorized and returns embeddings based
 * of chosen nvidia model.
 */
public class NVidiaEmbeddingClient implements EmbeddingService {
  private String apiKey;
  private String modelName;
  private String baseUrl;
  private final NVidiaEmbeddingService embeddingService;

  public NVidiaEmbeddingClient(String baseUrl, String apiKey, String modelName) {
    this.apiKey = apiKey;
    this.modelName = modelName;
    this.baseUrl = baseUrl;
    embeddingService =
        QuarkusRestClientBuilder.newBuilder()
            .baseUri(URI.create(baseUrl))
            .build(NVidiaEmbeddingService.class);
  }

  @RegisterRestClient
  public interface NVidiaEmbeddingService {
    @POST
    @ClientHeaderParam(name = "Content-Type", value = "application/json")
    Uni<EmbeddingResponse> embed(
        @HeaderParam("Authorization") String accessToken, EmbeddingRequest request);
  }

  private record EmbeddingRequest(String[] input, String model, String input_type) {}

  @JsonIgnoreProperties(ignoreUnknown = true)
  private record EmbeddingResponse(Data[] data, String model, Usage usage) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Data(int index, float[] embedding) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    private record Usage(int prompt_tokens, int total_tokens) {}
  }

  private static final String PASSAGE = "passage";
  private static final String QUERY = "query";

  @Override
  public Uni<List<float[]>> vectorize(
      List<String> texts,
      Optional<String> apiKeyOverride,
      EmbeddingRequestType embeddingRequestType) {
    String[] textArray = new String[texts.size()];
    String input_type = embeddingRequestType == EmbeddingRequestType.INDEX ? PASSAGE : QUERY;

    EmbeddingRequest request =
        new EmbeddingRequest(texts.toArray(textArray), modelName, input_type);
    Uni<EmbeddingResponse> response =
        embeddingService.embed(
            "Bearer " + (apiKeyOverride.isPresent() ? apiKeyOverride.get() : apiKey), request);
    return response
        .onItem()
        .transform(
            resp -> {
              Arrays.sort(resp.data(), (a, b) -> a.index() - b.index());
              return Arrays.stream(resp.data()).map(data -> data.embedding()).toList();
            });
  }
}
