package purposeawarekafka;

import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PurposeStore implements Runnable {
	private final GlobalKTable<String, IntendedPurposeDeclaration> table;
	private final KafkaStreams streams;

	public PurposeStore() {
		final var props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "pbac");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final var builder = new StreamsBuilder();

		this.table = builder.globalTable("reservations",
				Consumed.with(Serdes.String(), Serdes.serdeFrom(new JsonSerializer<>(),
						new JsonDeserializer<>(IntendedPurposeDeclaration.class))),
				Materialized.as("reservations-store"));

		final var topology = builder.build();
		this.streams = new KafkaStreams(topology, props);
	}

	public Collection<IntendedPurposeDeclaration> getIntendendedPurposes(String topic, String scope) {
		final var result = new ArrayList<IntendedPurposeDeclaration>();
		final var store = streams.store(
				StoreQueryParameters.fromNameAndType(
						table.queryableStoreName(),
						QueryableStoreTypes.<String, IntendedPurposeDeclaration>keyValueStore()));
		store.prefixScan(topic + "$" + scope + "$", new StringSerializer()).forEachRemaining(pair -> result.add(pair.value));
		return result;
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
			Thread.sleep(Duration.ofMinutes(2).toMillis());
			streams.start();
			latch.await();
		} catch (Throwable e) {
			streams.close();
		}
	}
}
