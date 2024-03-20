package io.stargate.sgv2.jsonapi.service.cqldriver.sstablewriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.params.shadow.com.univocity.parsers.conversions.Conversion;
import org.junit.jupiter.params.shadow.com.univocity.parsers.csv.CsvParser;
import org.junit.jupiter.params.shadow.com.univocity.parsers.csv.CsvParserSettings;

public class JacksonTests {
  public static void main(String[] args) throws IOException {
    parseArrayOfJson();
    System.out.println("=====================================");
    parseKeyValueJson();
    System.out.println("=====================================");
    readCSV();
  }

  private static void readCSV() throws JsonProcessingException {
    CsvParserSettings settings = new CsvParserSettings();
    CsvParser csvParser = new CsvParser(new CsvParserSettings());
    csvParser
        .iterateRecords(
            new File(
                Objects.requireNonNull(JacksonTests.class.getResource("/csv_records")).getFile()))
        .forEach(
            csvRecord -> {
              ObjectMapper objectMapper = new ObjectMapper();
              ObjectNode objectNode = objectMapper.createObjectNode();
              objectNode.set("_id", objectMapper.valueToTree(csvRecord.getValue(0, String.class)));
              objectNode.set("name", objectMapper.valueToTree(csvRecord.getValue(1, String.class)));
              try {
                ArrayNode addressNode = objectNode.arrayNode();
                String csvData =
                    csvRecord.getValue(
                        2,
                        String.class,
                        new Conversion<String, String>() {
                          @Override
                          public String execute(String o) {
                            return o.replaceFirst("\"", "").substring(0, o.length() - 1);
                          }

                          @Override
                          public String revert(String o) {
                            return o;
                          }
                        });
                JsonNode[] jsonNodeList = objectMapper.readValue(csvData, JsonNode[].class);
                for (JsonNode jsonNode : jsonNodeList) {
                  addressNode.add(jsonNode);
                }
                objectNode.set("address", addressNode);
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
              objectNode.set(
                  "phone", objectMapper.valueToTree(csvRecord.getValue(3, String.class)));
              objectNode.set(
                  "birthday", objectMapper.valueToTree(csvRecord.getValue(4, String.class)));
              objectNode.set(
                  "location", objectMapper.valueToTree(csvRecord.getValue(5, String.class)));
              System.out.println(objectNode);
            });
  }

  private static void parseKeyValueJson() throws IOException {
    String jsonObjects =
        """
                {
                    "1": {
                        "address": {
                            "city": "Anytown",
                            "country": "US",
                            "state": "Anystate",
                            "street": "123 Main St"
                        },
                        "birthday": {
                            "$date": 1358205756553
                        },
                        "location": {
                            "coordinates": [
                                -73.856077,
                                40.848447
                            ],
                            "type": "Point"
                        },
                        "name": "John Doe",
                        "phone": "555-123-4567"
                    },
                    "2": {
                        "name": "Jim Smith"
                    }
                }
                """;
    ObjectMapper objectMapper = new ObjectMapper();

    MappingIterator<Map<String, Object>> mappingIterator =
        objectMapper.readerForMapOf(Object.class).readValues(jsonObjects);
    while (mappingIterator.hasNext()) {
      Map<String, Object> entry = mappingIterator.next();
      // JsonNode jsonNode = objectMapper.readTree(json);
      JsonNode jsonNode = objectMapper.valueToTree(entry);
      System.out.println(jsonNode);
      SmallRyeConfig smallRyeConfig =
          new SmallRyeConfigBuilder().withMapping(DocumentLimitsConfig.class).build();
      DocumentLimitsConfig documentLimitsConfig =
          smallRyeConfig.getConfigMapping(DocumentLimitsConfig.class);
      Shredder shredder = new Shredder(objectMapper, documentLimitsConfig, null);
      WritableShreddedDocument writableShreddedDocument =
          shredder.shred(jsonNode, UUID.randomUUID());
      System.out.println(writableShreddedDocument);
    }
  }

  private static void parseArrayOfJson() throws JsonProcessingException {
    String jsonArray =
        """
                [{
                        "_id": "1",
                        "name": "John Doe",
                        "address": {
                            "city": "Anytown",
                            "country": "US",
                            "state": "Anystate",
                            "street": "123 Main St"
                        },
                        "phone": "555-123-4567",
                        "birthday": {
                            "$date": 1358205756553
                        },
                        "location": {
                            "coordinates": [
                                -73.856077,
                                40.848447
                            ],
                            "type": "Point"
                        }
                    },
                    {
                        "_id": "2",
                        "name": "Jim Smith"
                    }]

                """;
    ObjectMapper objectMapper = new ObjectMapper();
    for (JsonNode json : objectMapper.readValue(jsonArray, JsonNode[].class)) {
      // JsonNode jsonNode = objectMapper.readTree(json);
      JsonNode jsonNode = json;
      System.out.println(jsonNode);
      SmallRyeConfig smallRyeConfig =
          new SmallRyeConfigBuilder().withMapping(DocumentLimitsConfig.class).build();
      DocumentLimitsConfig documentLimitsConfig =
          smallRyeConfig.getConfigMapping(DocumentLimitsConfig.class);
      Shredder shredder = new Shredder(objectMapper, documentLimitsConfig, null);
      WritableShreddedDocument writableShreddedDocument =
          shredder.shred(jsonNode, UUID.randomUUID());
      System.out.println(writableShreddedDocument);
    }
  }
}
