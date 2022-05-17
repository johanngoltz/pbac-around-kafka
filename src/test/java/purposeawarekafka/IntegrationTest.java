package purposeawarekafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

public class IntegrationTest {
	public static final String TOPIC_NAME = "quickstart-events";

	@ClassRule
	public static DockerComposeContainer compose =
			new DockerComposeContainer(
					new File("src/test/resources/docker-compose.yml"))
					.withExposedService("pbac", 9093)
					.waitingFor("pbac", new LogMessageWaitStrategy().withRegEx(".*to RUNNING.*"));

	@Test
	public void createTopic() throws Exception {
		final var newTopicName = "integrationTest." + UUID.randomUUID();
		final var newTopic = new NewTopic(newTopicName, Optional.empty(), Optional.empty());

		final AdminClient adminClient = doCreateTopic(newTopic);

		final var existingTopics = adminClient.listTopics().names().get(10, SECONDS);

		assertTrue(existingTopics.contains(newTopicName));
	}

	@NotNull
	private AdminClient doCreateTopic(NewTopic newTopic) throws InterruptedException, ExecutionException,
			TimeoutException {
		final var adminClientConfig = new Properties();
		adminClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");

		final var adminClient = KafkaAdminClient.create(adminClientConfig);

		final var createTopics = adminClient.createTopics(List.of(newTopic));

		createTopics.all().get(10, SECONDS);
		return adminClient;
	}

	@Test
	public void passMessageNoIntendedPurposes() throws Exception {
		doCreateTopic(new NewTopic(TOPIC_NAME, Optional.empty(), Optional.empty()));

		final var latch = new CountDownLatch(10);

		consume(latch).start();
		produce(Stream
				.generate(() -> new MessageForDemo("user-" + UUID.randomUUID(), "DE", 12.5f))
				.limit(10))
				.start();

		assertTrue(latch.await(10, SECONDS));
	}

	private KafkaStreams consume(CountDownLatch latch) {
		final var props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getProxyHost());
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "integrationConsumer");

		final var builder = new StreamsBuilder();
		builder.stream(TOPIC_NAME, Consumed.with(Serdes.String(), new JsonSerde<>(MessageForDemo.class)))
				.peek((key, value) -> latch.countDown());

		final var topology = builder.build();

		return new KafkaStreams(topology, props);
	}

	@NotNull
	private Thread produce(Stream<MessageForDemo> messages) {
		return new Thread(() -> {
			final var producerConfig = new Properties();
			producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getProxyHost());

			try (final var producer = new KafkaProducer<String, MessageForDemo>(producerConfig,
					new JsonSerializer<>(),
					new JsonSerializer<>())) {

				messages.forEach(body -> producer.send(new ProducerRecord<>(TOPIC_NAME, body)));
				producer.flush();
			}
		});
	}

	private String getProxyHost() {
		return "%s:%d".formatted(
				compose.getServiceHost("pbac", 9093),
				compose.getServicePort("pbac", 9093));
	}
}
