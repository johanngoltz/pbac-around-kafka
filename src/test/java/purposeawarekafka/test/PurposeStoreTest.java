package purposeawarekafka.test;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.streams.KafkaStreams;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import purposeawarekafka.pbac.model.IntendedPurposeReservation;
import purposeawarekafka.pbac.model.IntendedPurposeReservationKey;
import purposeawarekafka.pbac.model.IntendedPurposeReservationValue;
import purposeawarekafka.pbac.PurposeStore;

import java.io.File;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

public class PurposeStoreTest {

	@ClassRule
	public static DockerComposeContainer compose =
			new DockerComposeContainer(
					new File("src/test/resources/regular-kafka/docker-compose.yml"))
					.withExposedService("kafka", 9092, new LogMessageWaitStrategy().withRegEx(".*Recorded new " +
							"controller.*"));


	@Test
	public void getIntendedPurposes() throws ExecutionException, InterruptedException, TimeoutException {
		final var bootstrapServer = compose.getServiceHost("kafka", 9092) + ":9092";

		final var streamsStart = new CountDownLatch(1);
		final var cut = new PurposeStore(bootstrapServer, (newState, oldState) -> {
			if (newState == KafkaStreams.State.RUNNING) {
				streamsStart.countDown();
			}
		});

		final var kafkaClientConfig = new Properties();
		kafkaClientConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

		AdminClient.create(kafkaClientConfig).createTopics(Collections.singleton(new NewTopic("ip-reservations", Optional.empty(), Optional.empty()))).all().get();

		new Thread(cut).start();

		streamsStart.await();

		final var randomTopicId = Uuid.randomUuid();
		final var userIdExtractors = List.of(".aProperty", ".aDifferentProperty", ".aAAProp", ".aPropertyButLonger");

		try (final var ipProducer =
				     new KafkaProducer<IntendedPurposeReservationKey, IntendedPurposeReservationValue>(kafkaClientConfig,
						     new JsonSerializer<>(),
						     new JsonSerializer<>())) {
			userIdExtractors.stream()
					.map(extractor ->
							new IntendedPurposeReservation("the-user", extractor, randomTopicId.toString(),
									Set.of(),
									Set.of())
					).map(ip ->
							new ProducerRecord<>("ip-reservations", ip.getKeyForPublish(),
									ip.getValueForPublish()))
					.forEach(ipProducer::send);
		}

		final var it = userIdExtractors.stream().sorted().iterator();
		cut.getIntendedPurposes(randomTopicId, intendedPurposeReservationKey -> {
			assertEquals(it.next(), intendedPurposeReservationKey.userIdExtractor());
			return Optional.empty();
		});
		assertFalse(it.hasNext());

	}
}