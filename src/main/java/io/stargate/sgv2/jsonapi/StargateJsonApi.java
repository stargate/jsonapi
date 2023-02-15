package io.stargate.sgv2.jsonapi;

import io.stargate.sgv2.api.common.config.constants.HttpConstants;
import io.stargate.sgv2.api.common.grpc.SourceApiQualifier;
import io.stargate.sgv2.jsonapi.config.constants.OpenApiConstants;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Application;
import org.eclipse.microprofile.openapi.annotations.Components;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.ParameterIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

@OpenAPIDefinition(
    // note that info is defined via the properties
    info = @Info(title = "", version = ""),
    tags = {
      @Tag(name = "General", description = "Executes general commands."),
      @Tag(name = "Namespaces", description = "Executes namespace commands."),
      @Tag(
          name = "Documents",
          description = "Executes document commands against a single collection."),
    },
    components =
        @Components(

            // security schemes
            securitySchemes = {
              @SecurityScheme(
                  securitySchemeName = OpenApiConstants.SecuritySchemes.TOKEN,
                  type = SecuritySchemeType.APIKEY,
                  in = SecuritySchemeIn.HEADER,
                  apiKeyName = HttpConstants.AUTHENTICATION_TOKEN_HEADER_NAME)
            },

            // reusable parameters
            parameters = {
              @Parameter(
                  in = ParameterIn.PATH,
                  name = "namespace",
                  required = true,
                  schema = @Schema(implementation = String.class, pattern = "\\w+"),
                  description = "The namespace where the collection is located.",
                  example = "cycling"),
              @Parameter(
                  in = ParameterIn.PATH,
                  name = "collection",
                  required = true,
                  schema = @Schema(implementation = String.class, pattern = "\\w+"),
                  description = "The name of the collection.",
                  example = "events")
            },

            // reusable examples
            examples = {
              @ExampleObject(
                  name = "findOne",
                  summary = "`findOne` command",
                  value =
                      """
                      {
                        "findOne": {
                            "filter": {"location": "London", "race.competitors" : {"$eq" : 100}}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "countDocuments",
                  summary = "`countDocuments` command",
                  value =
                      """
                            {
                              "countDocuments": {
                                  "filter": {"location": "London", "race.competitors" : {"$eq" : 100}}
                              }
                            }
                            """),
              @ExampleObject(
                  name = "find",
                  summary = "`find` command",
                  value =
                      """
                      {
                        "find": {
                             "filter": {"location": "London", "race.competitors" : {"$eq" : 100}},
                             "options": {"limit" : 1000, "pageSize": 25, "pagingState" : "Next paging state got from previous page call"}
                        }
                      }
                      """),
              @ExampleObject(
                  name = "findOneAndUpdate",
                  summary = "`findOneAndUpdate` command",
                  value =
                      """
                      {
                        "findOneAndUpdate": {
                            "filter": {"location": "London"},
                            "update": {
                                "$set": {"location": "New York"},
                                "$inc": {"count": 3}
                            }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "updateOne",
                  summary = "`updateOne` command",
                  value =
                      """
                      {
                      "updateOne": {
                          "filter": {"location": "London"},
                          "update": {
                              "$set": {"location": "New York"},
                              "$push": {"tags": "marathon"}
                          }
                      }
                    }
                    """),
              @ExampleObject(
                  name = "deleteOne",
                  summary = "`deleteOne` command",
                  value =
                      """
                            {
                              "deleteOne": {
                                  "filter": {"_id": "1"}
                              }
                            }
                            """),
              @ExampleObject(
                  name = "insertOne",
                  summary = "`insertOne` command",
                  value =
                      """
                      {
                        "insertOne": {
                          "document": {
                            "_id": "1",
                            "location": "London",
                            "race": {
                              "competitors": 100,
                              "start_date": "2022-08-15"
                            },
                            "tags": [ ]
                          }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "insertMany",
                  summary = "`insertMany` command",
                  value =
                      """
                                    {
                                      "insertMany": {
                                        "documents": [{
                                          "_id": "1",
                                          "location": "London",
                                          "race": {
                                            "competitors": 100,
                                            "start_date": "2022-08-15"
                                          },
                                          "tags" : [ "eu" ]
                                        },
                                        {
                                          "_id": "2",
                                          "location": "New York",
                                          "race": {
                                            "competitors": 150,
                                            "start_date": "2022-09-15"
                                          },
                                          "tags": [ "us" ]
                                        }]
                                      }
                                    }
                                    """),
              @ExampleObject(
                  name = "createNamespace",
                  summary = "`CreateNamespace` command",
                  value =
                      """
                            {
                                "createNamespace": {
                                  "name": "cycling"
                                }
                            }
                            """),
              @ExampleObject(
                  name = "createNamespaceWithReplication",
                  summary = "`CreateNamespace` command with replication",
                  value =
                      """
                            {
                                "createNamespace": {
                                  "name": "cycling",
                                  "options": {
                                    "replication": {
                                       "class": "SimpleStrategy",
                                       "replication_factor": 3
                                    }
                                  }
                                }
                            }
                            """),
              @ExampleObject(
                  name = "createCollection",
                  summary = "`CreateCollection` command",
                  value =
                      """
                            {
                                "createCollection": {
                                  "name": "events"
                                }
                            }
                            """),
              @ExampleObject(
                  name = "resultCount",
                  summary = "countDocuments command result",
                  value =
                      """
                                    {
                                      "status": {
                                        "counted_documents": 2
                                      }
                                    }
                                    """),
              @ExampleObject(
                  name = "resultRead",
                  summary = "Read command result",
                  value =
                      """
                      {
                        "data": {
                          "docs": [
                            {
                               "_id": "1",
                               "location": "London",
                               "race": {
                                 "competitors": 100,
                                 "start_date": "2022-08-15"
                               },
                               "tags": [ "eu" ]
                            },
                            {
                               "_id": "2",
                               "location": "Barcelona",
                               "race": {
                                 "competitors": 125,
                                 "start_date": "2022-09-26"
                               },
                               "tags": [ "us" ]
                            }
                          ],
                          "nextPageState": "jA8qg0AitZ8q28568GybNQ==",
                          "count": 2
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultFindOneAndUpdate",
                  summary = "`findOneAndUpdate` command result",
                  value =
                      """
                      {
                        "data": {
                          "docs": [
                            {
                               "_id": "1",
                               "location": "New York",
                               "race": {
                                 "competitors": 100,
                                 "start_date": "2022-08-15"
                               },
                               "tags": [ "eu" ],
                               "count": 3
                            }
                          ],
                          "count": 1,
                          "status": {
                            "updatedIds": ["1"]
                          }
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultUpdateOne",
                  summary = "`updateOne` command result",
                  value =
                      """
                      {
                          "status": {
                              "updatedIds": ["1"]
                            }
                          }
                        }
                        """),
              @ExampleObject(
                  name = "resultInsert",
                  summary = "Insert command result",
                  value =
                      """
                      {
                        "status": {
                            "insertedIds": ["1", "2"]
                        }
                      }
                      """),
              @ExampleObject(
                  name = "resultDelete",
                  summary = "Delete command result",
                  value =
                      """
                                {
                                  "status": {
                                      "deletedIds": ["1", "2"]
                                  }
                                }
                                """),
              @ExampleObject(
                  name = "resultError",
                  summary = "Error result",
                  value =
                      """
                      {
                        "errors": [
                          {
                            "message": "The command failed because of some specific reason."
                          }
                        ]
                      }
                      """),
              @ExampleObject(
                  name = "resultCreate",
                  summary = "Create result",
                  value =
                      """
                      {
                        "status": {
                            "ok": 1
                        }
                      }
                      """),
            }))
public class StargateJsonApi extends Application {

  @Produces
  @SourceApiQualifier
  public String sourceApi() {
    return "rest";
  }
}
