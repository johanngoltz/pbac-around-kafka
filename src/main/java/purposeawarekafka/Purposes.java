package purposeawarekafka;

import com.fasterxml.jackson.core.JsonParseException;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.input.CharSequenceReader;
import org.apache.kafka.common.Uuid;
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
			new AccessPurposeKey("marketing-consumer", "bench"), "marketing.email",
			new AccessPurposeKey("billing-consumer", "bench"), "billing");
	private final Map<IntendedPurposeScope, String> intendedPurpose = Map.of(
			new IntendedPurposeScope("user-1234", "quickstart-events", "marketing.email"), ".country == \"DE\"",
			new IntendedPurposeScope("user-6789", "quickstart-events", "billing"), ".country == \"PL\""
	);

	private record AccessPurposeKey(String clientId, String topicName) {}

	private final Set<String> topicNamesToExcludeFromPBAC = Collections.synchronizedSet(
			new HashSet<>(List.of("reservations")));

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
		if (!isRequestExemptFromPbac(requestHeader)) {
			if (response.data() instanceof FetchResponseData fetchResponseData) {
				for (final var fetchableTopicResponse : fetchResponseData.responses()) {
					final var topicId = fetchableTopicResponse.topicId();
					final var declaredPurpose =
							purposeStore.getAccessPurpose(new AccessPurposeDeclarationKey(
									topicId.toString(), requestHeader.clientId()));
					if (declaredPurpose.isPresent())
						for (final var partition : fetchableTopicResponse.partitions()) {
							final var records = (MemoryRecords) partition.records();
							for (Record record : records.records()) {
								makeRecordCompliant(declaredPurpose.get().accessPurpose(), topicId, record);
							}
						}
				}
			} else {
				throw new UnsupportedOperationException(response.getClass().getName() + " is not an instance of " +
						"FetchResponseData. Cannot apply PBAC.");
			}
		}
	}

	private boolean isRequestExemptFromPbac(RequestHeader requestHeader) {
		return requestHeader.clientId().startsWith("pbac");
	}

	private void makeRecordCompliant(String declaredPurpose, Uuid topicId, Record record) throws IOException {
		final var buffer = record.value();
		buffer.mark();

		final var reader = new CharSequenceReader(StandardCharsets.UTF_8.newDecoder().decode(buffer));

		final var isCompliant = isRecordCompliant(declaredPurpose, topicId, reader);

		buffer.reset();
		if (!isCompliant) {
			while (buffer.hasRemaining())
				buffer.put((byte) '-');
			buffer.rewind();
		}
	}

	private boolean isRecordCompliant(String declaredPurpose, Uuid topicId, CharSequenceReader reader) {
		final var maybeReservation = purposeStore.getIntendedPurposes(
				topicId,
				reservationKey -> maybeGetUserId(reader, reservationKey));

		if (maybeReservation.isEmpty()) {
			return true;
		} else {
			final var reservation = maybeReservation.get();
			return reservation.allowed().contains(declaredPurpose) &&
					!reservation.prohibited().contains(declaredPurpose);
		}

	}

	private Optional<String> maybeGetUserId(CharSequenceReader reader, IntendedPurposeReservationKey reservationKey) {
		String userId = null;
		try {
			userId = jq.evaluate(reservationKey.userIdExtractor(), reader).asText();
		} catch (JsonParseException e) {
			if (log.isTraceEnabled())
				log.trace("Could not evaluate " + reservationKey.userIdExtractor() + " on " + reader);
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			reader.reset();
		}
		return Optional.ofNullable(userId);
	}
}
