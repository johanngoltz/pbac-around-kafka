package purposeawarekafka;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import org.apache.commons.io.input.CharSequenceReader;

import java.io.IOException;
import java.io.Reader;
import java.util.LinkedList;

public class jq {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final Scope rootScope = Scope.newEmptyScope();

	public jq() {
		BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_6, rootScope);
	}

	public boolean evaluateToBool(String jqFilter, Reader json) throws IOException {
		try {
			final var result = evaluate(jqFilter, json);
			assert result.isBoolean();
			return result.booleanValue();
		} catch (JsonParseException ex) {
			return false;
		}
	}

	public JsonNode evaluate(String jqFilter, Reader json) throws IOException {
		final var childScope = Scope.newChildScope(rootScope);
		final var query = JsonQuery.compile(jqFilter, Versions.JQ_1_6);
		final var tree = objectMapper.readTree(json);
		final var results = new LinkedList<JsonNode>();
		query.apply(childScope, tree, results::add);
		return results.get(0);
	}
}
