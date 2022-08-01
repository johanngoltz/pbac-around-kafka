package purposeawarekafka;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Versions;
import org.apache.commons.io.input.CharSequenceReader;

import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class jq {
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final Scope rootScope = Scope.newEmptyScope();
	private final Scope scope = Scope.newChildScope(rootScope);

	private final Map<String, JsonQuery> queries = Collections.synchronizedMap(new HashMap<>());

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

	@SneakyThrows
	private JsonQuery compileQuery(String query) {
		return JsonQuery.compile(query, Versions.JQ_1_6);
	}

	public JsonNode evaluate(String jqFilter, Reader json) throws IOException {
		final var results = new LinkedList<JsonNode>();

		final var query = queries.computeIfAbsent(jqFilter, this::compileQuery);
		final var tree = objectMapper.readTree(json);

		query.apply(scope, tree, results::add);

		return results.get(0);
	}
}
