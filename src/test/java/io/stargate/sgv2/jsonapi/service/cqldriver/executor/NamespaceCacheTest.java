package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultColumnMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectMock;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.helpers.test.UniAssertSubscriber;
import io.stargate.sgv2.common.testprofiles.NoGlobalResourcesTestProfile;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoGlobalResourcesTestProfile.Impl.class)
public class NamespaceCacheTest {

  @Inject ObjectMapper objectMapper;

  @InjectMock protected DataApiRequestInfo dataApiRequestInfo;

  @Nested
  class Execute {

    @Test
    public void checkValidJsonApiTable() {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.getSchema(any(), any(), any()))
          .then(
              i -> {
                List<ColumnMetadata> partitionColumn =
                    Lists.newArrayList(
                        new DefaultColumnMetadata(
                            CqlIdentifier.fromInternal("ks"),
                            CqlIdentifier.fromInternal("table"),
                            CqlIdentifier.fromInternal("key"),
                            DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT),
                            false));
                Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
                columns.put(
                    CqlIdentifier.fromInternal("tx_id"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("tx_id"),
                        DataTypes.TIMEUUID,
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("doc_json"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("doc_json"),
                        DataTypes.TEXT,
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("exist_keys"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("exist_keys"),
                        DataTypes.setOf(DataTypes.TEXT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("array_size"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("array_size"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("array_contains"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("array_contains"),
                        DataTypes.setOf(DataTypes.TEXT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_bool_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_bool_values"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.TINYINT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_dbl_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_dbl_values"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.DECIMAL),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_text_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_text_values"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_timestamp_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_timestamp_values"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.TIMESTAMP),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_null_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_null_values"),
                        DataTypes.setOf(DataTypes.TEXT),
                        false));

                return Uni.createFrom()
                    .item(
                        Optional.of(
                            new DefaultTableMetadata(
                                CqlIdentifier.fromInternal("ks"),
                                CqlIdentifier.fromInternal("table"),
                                UUID.randomUUID(),
                                false,
                                false,
                                partitionColumn,
                                new HashMap<>(),
                                columns,
                                new HashMap<>(),
                                new HashMap<>())));
              });
      NamespaceCache namespaceCache = new NamespaceCache("ks", queryExecutor, objectMapper);
      CollectionSettings collectionSettings =
          namespaceCache
              .getCollectionProperties(dataApiRequestInfo, "table")
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(collectionSettings)
          .satisfies(
              s -> {
                assertThat(s.vectorEnabled()).isFalse();
                assertThat(s.collectionName()).isEqualTo("table");
              });
    }

    @Test
    public void checkValidJsonApiTableWithIndexing() {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.getSchema(any(), any(), any()))
          .then(
              i -> {
                List<ColumnMetadata> partitionColumn =
                    Lists.newArrayList(
                        new DefaultColumnMetadata(
                            CqlIdentifier.fromInternal("ks"),
                            CqlIdentifier.fromInternal("table"),
                            CqlIdentifier.fromInternal("key"),
                            DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT),
                            false));
                Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
                columns.put(
                    CqlIdentifier.fromInternal("tx_id"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("tx_id"),
                        DataTypes.TIMEUUID,
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("doc_json"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("doc_json"),
                        DataTypes.TEXT,
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("exist_keys"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("exist_keys"),
                        DataTypes.setOf(DataTypes.TEXT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("array_size"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("array_size"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.INT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("array_contains"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("array_contains"),
                        DataTypes.setOf(DataTypes.TEXT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_bool_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_bool_values"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.TINYINT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_dbl_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_dbl_values"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.DECIMAL),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_text_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_text_values"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_timestamp_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_timestamp_values"),
                        DataTypes.mapOf(DataTypes.TEXT, DataTypes.TIMESTAMP),
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("query_null_values"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("query_null_values"),
                        DataTypes.setOf(DataTypes.TEXT),
                        false));

                return Uni.createFrom()
                    .item(
                        Optional.of(
                            new DefaultTableMetadata(
                                CqlIdentifier.fromInternal("ks"),
                                CqlIdentifier.fromInternal("table"),
                                UUID.randomUUID(),
                                false,
                                false,
                                partitionColumn,
                                new HashMap<>(),
                                columns,
                                Map.of(
                                    CqlIdentifier.fromInternal("comment"),
                                    "{\"indexing\":{\"deny\":[\"comment\"]}}"),
                                new HashMap<>())));
              });
      NamespaceCache namespaceCache = new NamespaceCache("ks", queryExecutor, objectMapper);
      CollectionSettings collectionSettings =
          namespaceCache
              .getCollectionProperties(dataApiRequestInfo, "table")
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitItem()
              .getItem();

      assertThat(collectionSettings)
          .satisfies(
              s -> {
                assertThat(s.vectorEnabled()).isFalse();
                assertThat(s.collectionName()).isEqualTo("table");
                assertThat(s.indexingConfig().denied()).containsExactly("comment");
              });
    }

    @Test
    public void checkInvalidJsonApiTable() {
      QueryExecutor queryExecutor = mock(QueryExecutor.class);
      when(queryExecutor.getSchema(any(), any(), any()))
          .then(
              i -> {
                List<ColumnMetadata> partitionColumn =
                    Lists.newArrayList(
                        new DefaultColumnMetadata(
                            CqlIdentifier.fromInternal("ks"),
                            CqlIdentifier.fromInternal("table"),
                            CqlIdentifier.fromInternal("key"),
                            DataTypes.tupleOf(DataTypes.TINYINT, DataTypes.TEXT),
                            false));
                Map<CqlIdentifier, ColumnMetadata> columns = new HashMap<>();
                columns.put(
                    CqlIdentifier.fromInternal("tx_id"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("tx_id"),
                        DataTypes.TIMEUUID,
                        false));
                columns.put(
                    CqlIdentifier.fromInternal("doc"),
                    new DefaultColumnMetadata(
                        CqlIdentifier.fromInternal("ks"),
                        CqlIdentifier.fromInternal("table"),
                        CqlIdentifier.fromInternal("doc"),
                        DataTypes.TEXT,
                        false));
                return Uni.createFrom()
                    .item(
                        Optional.of(
                            new DefaultTableMetadata(
                                CqlIdentifier.fromInternal("ks"),
                                CqlIdentifier.fromInternal("table"),
                                UUID.randomUUID(),
                                false,
                                false,
                                partitionColumn,
                                new HashMap<>(),
                                columns,
                                new HashMap<>(),
                                new HashMap<>())));
              });
      NamespaceCache namespaceCache = new NamespaceCache("ks", queryExecutor, objectMapper);
      Throwable error =
          namespaceCache
              .getCollectionProperties(dataApiRequestInfo, "table")
              .subscribe()
              .withSubscriber(UniAssertSubscriber.create())
              .awaitFailure()
              .getFailure();

      assertThat(error)
          .isInstanceOfSatisfying(
              JsonApiException.class,
              s -> {
                assertThat(s.getErrorCode()).isEqualTo(ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA);
                assertThat(s.getMessage())
                    .isEqualTo(ErrorCode.INVALID_JSONAPI_COLLECTION_SCHEMA.getMessage() + "table");
              });
    }
  }
}
