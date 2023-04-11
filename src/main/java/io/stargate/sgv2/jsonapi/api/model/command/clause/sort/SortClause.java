package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.SortClauseDeserializer;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Internal model for the sort clause that can be used in the commands.
 *
 * @param sortExpressions Ordered list of sort expressions.
 */
@JsonDeserialize(using = SortClauseDeserializer.class)
@Schema(
    type = SchemaType.OBJECT,
    implementation = Map.class,
    example = """
              {"user.age" : -1, "user.name" : 1}
              """)
public record SortClause(@Valid List<SortExpression> sortExpressions) {}
