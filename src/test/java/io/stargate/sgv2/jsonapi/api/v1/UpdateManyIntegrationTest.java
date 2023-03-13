package io.stargate.sgv2.jsonapi.api.v1;

import static io.restassured.RestAssured.given;
import static io.stargate.sgv2.common.IntegrationTestUtils.getAuthToken;
import static net.javacrumbs.jsonunit.JsonMatchers.jsonEquals;
import static org.hamcrest.Matchers.everyItem;
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
public class UpdateManyIntegrationTest extends CollectionResourceBaseIntegrationTest {
  @Nested
  class UpdateMany {

    private void insert(int countOfDocument) {
      for (int i = 1; i <= countOfDocument; i++) {
        String json =
            """
            {
              "insertOne": {
                "document": {
                  "_id": "doc%s",
                  "username": "user%s",
                  "active_user" : true
                }
              }
            }
            """;
        given()
            .header(HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME, getAuthToken())
            .contentType(ContentType.JSON)
            .body(json.formatted(i, i))
            .when()
            .post(CollectionResource.BASE_PATH, keyspaceId.asInternal(), collectionName)
            .then()
            .statusCode(200);
      }
    }

    @Test
    public void updateManyById() {
      insert(2);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id" : "doc1"},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(1))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

      // assert state after update, first changed document
      String expected =
          """
          {
            "_id":"doc1",
            "username":"user1",
            "active_user":false
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc1"}
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
          .body("data.docs[0]", jsonEquals(expected));

      // then not changed document
      expected =
          """
          {
            "_id":"doc2",
            "username":"user2",
            "active_user":true
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc2"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void updateManyByColumn() {
      insert(5);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"active_user": true},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(5))
          .body("status.modifiedCount", is(5))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

      // assert all updated
      json = """
          {
            "find": {
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
          .body("data.docs.active_user", everyItem(is(false)));
    }

    @Test
    public void updateManyLimit() {
      insert(20);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"active_user": true},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"active_user": false}
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
          .body("data.docs.active_user", everyItem(is(false)))
          .body("data.count", is(20))
          .body("errors", is(nullValue()));
    }

    @Test
    public void updateManyLimitMoreDataFlag() {
      insert(25);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"active_user" : true},
              "update" : {"$set" : {"active_user": false}}
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
          .body("status.matchedCount", is(20))
          .body("status.modifiedCount", is(20))
          .body("status.moreData", is(true))
          .body("errors", is(nullValue()));

      json =
          """
          {
            "find": {
              "filter" : {"active_user": true}
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
          .body("data.docs.active_user", everyItem(is(true)))
          .body("data.count", is(5));
    }

    @Test
    public void updateManyUpsert() {
      insert(5);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id": "doc6"},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

      // assert upsert
      String expected =
          """
          {
            "_id":"doc6",
            "active_user":false
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc6"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void updateManyByIdNoChange() {
      insert(2);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id" : "doc1"},
              "update" : {"$set" : {"active_user": true}}
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
          .body("status.matchedCount", is(1))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

      String expected =
          """
          {
            "_id":"doc1",
            "username":"user1",
            "active_user":true
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc1"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }

    @Test
    public void updateManyUpsertAddFilterColumn() {
      insert(5);
      String json =
          """
          {
            "updateMany": {
              "filter" : {"_id": "doc6", "answer" : 42},
              "update" : {"$set" : {"active_user": false}},
              "options" : {"upsert" : true}
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
          .body("status.upsertedId", is("doc6"))
          .body("status.matchedCount", is(0))
          .body("status.modifiedCount", is(0))
          .body("status.moreData", nullValue())
          .body("errors", is(nullValue()));

      // assert state after update
      String expected =
          """
          {
            "_id":"doc6",
            "answer": 42,
            "active_user": false
          }
          """;
      json =
          """
          {
            "find": {
              "filter" : {"_id" : "doc6"}
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
          .body("data.docs[0]", jsonEquals(expected));
    }
  }

  @AfterEach
  public void cleanUpData() {
    deleteAllDocuments();
  }
}
