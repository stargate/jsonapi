package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.restassured.http.ContentType;
import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.jsonapi.testresource.DseTestResource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@QuarkusTestResource(DseTestResource.class)
public class FindOneWithProjectionIntegrationTest extends CollectionResourceBaseIntegrationTest {
  private static final String DOC1_JSON =
      """
                {
                  "_id": "doc1",
                  "username": "user1",
                  "active_user" : true
                }
                """;

  private static final String DOC2_JSON =
      """
                {
                  "_id": "doc2",
                  "username": "user2",
                  "tags" : ["tag1", "tag2", "tag42", "tag1972", "zzzz"],
                  "nestedArray" : [["tag1", "tag2"], ["tag3", null]]
                }
                """;

  private static final String DOC3_JSON =
      """
                {
                  "_id": "doc3",
                  "username": "user3",
                  "sub_doc" : { "a": 5, "b": { "c": "v1", "d": false } }
                }
                """;

  @Nested
  class BasicProjection {
    @Test
    public void byIdNestedExclusion() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
          {
            "findOne": {
              "filter" : {"_id" : "doc3"},
              "projection": { "sub_doc.b": 0 }
            }
          }
          """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body(
              "data.docs[0]",
              jsonEquals(
                  """
                                        {
                                          "_id": "doc3",
                                          "username": "user3",
                                          "sub_doc" : { "a": 5 }
                                        }
                                        """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @AfterEach
    public void cleanUpData() {
      deleteAllDocuments();
    }
  }

  @Nested
  class ProjectionWithSlice {
    @Test
    public void byIdRootSliceHead() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
                  {
                    "findOne": {
                      "filter" : {"_id" : "doc2"},
                      "projection": { "tags": { "$slice" : 2 }  }
                    }
                  }
                  """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body(
              "data.docs[0]",
              jsonEquals(
                  """
                              {
                                "_id": "doc2",
                                "username": "user2",
                                "tags" : ["tag1", "tag2"],
                                "nestedArray" : [["tag1", "tag2"], ["tag3", null]]
                              }
                              """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @Test
    public void byIdRootSliceTail() {
      insertDoc(DOC1_JSON);
      insertDoc(DOC2_JSON);
      insertDoc(DOC3_JSON);
      String json =
          """
                      {
                        "findOne": {
                          "filter" : {"_id" : "doc2"},
                          "projection": { "tags": { "$slice" : -2 }  }
                        }
                      }
                      """;

      given()
          .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
          .contentType(ContentType.JSON)
          .body(json)
          .when()
          .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
          .then()
          .statusCode(200)
          .body("data.count", is(1))
          .body("data.docs", hasSize(1))
          .body(
              "data.docs[0]",
              jsonEquals(
                  """
                                          {
                                            "_id": "doc2",
                                            "username": "user2",
                                            "tags" : ["tag1972", "zzzz"],
                                            "nestedArray" : [["tag1", "tag2"], ["tag3", null]]
                                          }
                                          """))
          .body("status", is(nullValue()))
          .body("errors", is(nullValue()));
    }

    @AfterEach
    public void cleanUpData() {
      deleteAllDocuments();
    }
  }
}