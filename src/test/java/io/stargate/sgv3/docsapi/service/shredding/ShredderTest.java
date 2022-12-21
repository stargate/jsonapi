package io.stargate.sgv3.docsapi.service.shredding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.sgv3.docsapi.service.shredding.model.WritableShreddedDocument;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ShredderTest {
  @Inject ObjectMapper objectMapper;

  private final Shredder shredder = new Shredder();

  @Test
  public void simpleShredFromPathExample() throws Exception {
    final String JSON =
        """
{ "_id" : "abc",
  "name" : "Bob",
  "values" : [ 1, 2 ],
  "[extra.stuff]" : true,
  "nullable" : null
}
                """;
    WritableShreddedDocument doc = shredder.shred(objectMapper.readTree(JSON));
    assertThat(doc.id()).isEqualTo("abc");
    List<JSONPath> expPaths =
        Arrays.asList(
            JSONPath.fromEncoded("name"),
            JSONPath.fromEncoded("values"),
            JSONPath.fromEncoded("values.[0]"),
            JSONPath.fromEncoded("values.[1]"),
            JSONPath.fromEncoded("\\[extra\\.stuff]"),
            JSONPath.fromEncoded("nullable"));

    // First verify paths
    assertThat(doc.docFieldOrder()).isEqualTo(expPaths);
    assertThat(doc.existKeys()).isEqualTo(new HashSet<>(expPaths));

    // Atomic value counts (5 atomic fields, _id not included)
    assertThat(doc.docAtomicFields()).hasSize(5);

    // Then array info (doc has one array, with 2 elements)
    assertThat(doc.arraySize())
        .isEqualTo(Collections.singletonMap(JSONPath.fromEncoded("values"), Integer.valueOf(2)));
    assertThat(doc.arrayEquals()).hasSize(1);
    assertThat(doc.arrayContains()).hasSize(2);

    // Sub-documents (Object values): none in this example
    assertThat(doc.subDocEquals()).hasSize(0);

    // Then atomic value containers
    assertThat(doc.queryBoolValues())
        .isEqualTo(
            Collections.singletonMap(JSONPath.fromEncoded("\\[extra\\.stuff]"), Boolean.TRUE));
    Map<JSONPath, BigDecimal> expNums = new LinkedHashMap<>();
    expNums.put(JSONPath.fromEncoded("values.[0]"), BigDecimal.valueOf(1));
    expNums.put(JSONPath.fromEncoded("values.[1]"), BigDecimal.valueOf(2));
    assertThat(doc.queryNumberValues()).isEqualTo(expNums);
    assertThat(doc.queryTextValues())
        .isEqualTo(Collections.singletonMap(JSONPath.fromEncoded("name"), "Bob"));
    assertThat(doc.queryNullValues())
        .isEqualTo(Collections.singleton(JSONPath.fromEncoded("nullable")));
  }

  @Test
  public void docBadJSONType() throws Exception {
    final String JSON = "[ 1, 2 ]";
    Throwable t = catchThrowable(() -> shredder.shred(objectMapper.readTree(JSON)));
    assertThat(t)
        .isNotNull()
        .hasMessageContaining("Document to shred must be a JSON Object: instead got ARRAY");
  }

  @Test
  public void docBadDocIdType() {
    final String JSON = """
{ "_id" : [ ] }
                """;
    Throwable t = catchThrowable(() -> shredder.shred(objectMapper.readTree(JSON)));
    assertThat(t).isNotNull().hasMessageContaining("Bad type for '_id' property (ARRAY)");
  }
}
