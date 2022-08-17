package purposeawarekafka;

import com.fasterxml.jackson.core.JsonParseException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.CharSequenceReader;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Slf4j
public class Purposes {
	private final Collection<ApiKeys> relevantApiKeys = List.of(ApiKeys.FETCH); //ApiKeys.OFFSET_FETCH
	private final jq jq = new jq();
	private final Map<AccessPurposeKey, String> declaredPurposes = Map.of(
			new AccessPurposeKey("bench", "marketing-consumer"), "marketing.email",
			new AccessPurposeKey("bench", "billing-consumer"), "billing");
	private final Map<IntendedPurposeScope, String> intendedPurpose = Map.of(
			new IntendedPurposeScope("user-1234", "quickstart-events", "marketing.email"), ".country == \"DE\"",
			new IntendedPurposeScope("user-6789", "quickstart-events", "billing"), ".country == \"PL\""
	);

	private record AccessPurposeKey(String clientId, String topicName) { }
	private final Set<String> topicNamesToExcludeFromPBAC = Collections.synchronizedSet(new HashSet<>(List.of("reservations")));

	private final PurposeStore purposeStore;

	public Purposes(PurposeStore purposeStore) {
		this.purposeStore = purposeStore;
	}

	private final record IntendedPurposeScope(String userId, String topic, String purpose) {}


	public boolean isRequestPurposeRelevant(RequestHeader requestHeader) {
		return relevantApiKeys.contains(requestHeader.apiKey());
	}

	@SneakyThrows
	public void makeResponsePurposeCompliant(RequestHeader requestHeader, AbstractResponse response) {
		if (response.data() instanceof FetchResponseData fetchResponseData) {
			for (final var fetchableTopicResponse : fetchResponseData.responses()) {
				final var topicName = fetchableTopicResponse.topic();
				final var declaredPurpose = declaredPurposes.get(new AccessPurposeKey(requestHeader.clientId(), topicName));
				if (!topicNamesToExcludeFromPBAC.contains(topicName)) {
					for (final var partition : fetchableTopicResponse.partitions()) {
						final var records = (MemoryRecords) partition.records();
						for (Record record : records.records()) {
							makeRecordCompliant(declaredPurpose, topicName, record);
						}
					}
				}
			}
		} else {
			throw new UnsupportedOperationException(response.getClass().getName() + " is not an instance of FetchResponseData. Cannot apply PBAC.");
		}
	}

	private void makeRecordCompliant(String declaredPurpose, String topic, Record record) throws IOException {
		final var buffer = record.value();
		buffer.mark();
		final var reader = new CharSequenceReader(StandardCharsets.UTF_8.newDecoder().decode(buffer));

		var isCompliant = false;
		try {
			final var scope = jq.evaluate(".userId", reader).asText();
			reader.reset();
			final var intendedPurposes = purposeStore.getIntendendedPurposes(topic, scope);
			if (intendedPurposes.isEmpty()) {
				isCompliant = true;
			} else {
				final var jqFilter = intendedPurposes.iterator().next().condition();
				// log.info("Evaluating " + jqFilter);
				isCompliant = jq.evaluateToBool(jqFilter, reader);
			}
		} catch (JsonParseException e) {
			e.printStackTrace();
		}
		if (!isCompliant) {
			buffer.reset();
			while (buffer.hasRemaining())
				buffer.put((byte) '-');
			buffer.rewind();
		}
	}
}
