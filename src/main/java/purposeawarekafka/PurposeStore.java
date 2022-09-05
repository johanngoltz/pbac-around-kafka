package purposeawarekafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

@Slf4j
public class PurposeStore implements Runnable {
	private final GlobalKTable<IntendedPurposeReservationKey, IntendedPurposeReservationValue> ipTable;
	private final GlobalKTable<AccessPurposeDeclarationKey, AccessPurposeDeclarationValue> apTable;
	private final KafkaStreams streams;
	private final JsonSerializer<IntendedPurposeReservationKey> jsonSerializer = new JsonSerializer<>();

	public PurposeStore(String bootstrapServer) {this(bootstrapServer, null);}

	public PurposeStore(String bootstrapServer, KafkaStreams.StateListener stateListener) {
		final var props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pbac");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final var builder = new StreamsBuilder();

		this.ipTable = builder.globalTable("ip-reservations",
				Consumed.with(
						new JsonSerde<>(IntendedPurposeReservationKey.class).ignoreTypeHeaders(),
						new JsonSerde<>(IntendedPurposeReservationValue.class)),
				Materialized.as("ip-reservations-store"));
		this.apTable = builder.globalTable("ap-declarations",
				Consumed.with(
						new JsonSerde<>(AccessPurposeDeclarationKey.class).ignoreTypeHeaders(),
						new JsonSerde<>(AccessPurposeDeclarationValue.class)),
				Materialized.as("ap-reservations-store"));

		final var topology = builder.build();
		this.streams = new KafkaStreams(topology, props);
		streams.cleanUp(); // mainly for testing, should not hurt in prod
		if (stateListener != null) streams.setStateListener(stateListener);
	}

	public Optional<IntendedPurposeReservation> getIntendedPurposes(
			Uuid topicId,
			Function<IntendedPurposeReservationKey, Optional<String>> extractUserId) {
		final var store = streams.store(StoreQueryParameters.fromNameAndType(
				ipTable.queryableStoreName(),
				QueryableStoreTypes.<IntendedPurposeReservationKey, IntendedPurposeReservationValue>keyValueStore()));

		var scanner = store.prefixScan(
				new IntendedPurposeReservationKey("", "", topicId.toString()),
				(topic, data) -> "{\"topic\":\"%s\"".formatted(data.topic()).getBytes(StandardCharsets.UTF_8));
		try {
			while (scanner.hasNext()) {
				final var nextKey = scanner.peekNextKey();
				scanner.close();

				// The extractor may or may not be applicable to the message
				final var maybeUserId = extractUserId.apply(nextKey);

				if (maybeUserId.isPresent()) {
					// The extractor is applicable. We know the ID of the affected user.
					// If the user made a reservation for this topic with the same extractor, return it.
					return maybeGetExactReservation(store, nextKey, maybeUserId);
				} else {
					// Extractor is not applicable - move to next extractor
					final var userIdExtractorPlusOne = nextKey.userIdExtractor() + (char) ((byte) '"' + 1);
					final var nextKeyPlusOne = new IntendedPurposeReservationKey("",
							userIdExtractorPlusOne, nextKey.topic());
					scanner = store.range(nextKeyPlusOne, null);
				}
			}

			return Optional.empty();
		} finally {
			scanner.close();
		}
	}

	private Optional<IntendedPurposeReservation> maybeGetExactReservation(ReadOnlyKeyValueStore<IntendedPurposeReservationKey,
			IntendedPurposeReservationValue> store, IntendedPurposeReservationKey nextKey,
	                                                                      Optional<String> maybeUserId) {
		final var userId = maybeUserId.get();
		final var applicableReservationKey = new IntendedPurposeReservationKey(
				userId,
				nextKey.userIdExtractor(),
				nextKey.topic());
		final var reservation = store.get(applicableReservationKey);
		if (reservation == null)
			return Optional.empty();
		return Optional.of(IntendedPurposeReservation.fromKeyValue(applicableReservationKey, reservation));
	}

	public Optional<AccessPurposeDeclaration> getAccessPurpose(AccessPurposeDeclarationKey key) {
		final var store = streams.store(StoreQueryParameters.fromNameAndType(
				apTable.queryableStoreName(),
				QueryableStoreTypes.<AccessPurposeDeclarationKey, AccessPurposeDeclarationValue>keyValueStore()));
		final var value = store.get(key);
		if (value != null) return Optional.of(AccessPurposeDeclaration.fromKeyValue(key, value));
		else {
			return Optional.empty();
		}
	}

	@Override
	public void run() {
		final var latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
			@Override
			public void run() {
				streams.close();
				latch.countDown();
			}
		});

		try {
			streams.start();
			latch.await();
		} catch (Throwable e) {
			streams.close();
		}
	}
}
