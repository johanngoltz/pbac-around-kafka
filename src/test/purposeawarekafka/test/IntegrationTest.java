package purposeawarekafka.test;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import purposeawarekafka.AccessPurposeDeclaration;
import purposeawarekafka.IntendedPurposeReservation;
import purposeawarekafka.MessageForDemo;

import java.io.File;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

public class IntegrationTest {
	public static final String TOPIC_NAME = "quickstart-events";

	@ClassRule
	public static DockerComposeContainer compose =
			new DockerComposeContainer(
					new File("src/test/resources/docker-compose.yml"))
					.withExposedService("pbac", 9093)
					.waitingFor("kafka", new LogMessageWaitStrategy().withRegEx(".*Recorded new controller.*"))
					.waitingFor("pbac",
							new LogMessageWaitStrategy().withRegEx(".*to RUNNING.*").withStartupTimeout(ofSeconds(180)));
	private final TestUtils testUtils = new TestUtils();

	@Test
	public void createTopic() throws Exception {
		final var newTopicName = "integrationTest." + UUID.randomUUID();
		final var newTopic = new NewTopic(newTopicName, Optional.empty(), Optional.empty());

		final AdminClient adminClient = testUtils.doCreateTopic(newTopic);

		final var existingTopics = adminClient.listTopics().names().get(10, SECONDS);

		assertTrue(existingTopics.contains(newTopicName));
	}

	@Test
	public void passMessageNoIntendedPurposes() throws Exception {
		testUtils.doCreateTopic(new NewTopic(TOPIC_NAME, Optional.empty(), Optional.empty()));

		final var latch = new CountDownLatch(10);

		testUtils.consume(compose, TOPIC_NAME, (key, value) -> latch.countDown()).start();
		testUtils.produce(compose, TOPIC_NAME, Stream
								.generate(() -> new MessageForDemo("user-" + UUID.randomUUID(), "DE", 12.5f))
								.limit(10),
						null)
				.start();

		assertTrue(latch.await(100, SECONDS));
	}

	@Test
	public void passMessageAllowedByIntendedPurposes() throws Exception {
		testUtils.doCreateTopic(new NewTopic(TOPIC_NAME, Optional.empty(), Optional.empty()));


		final var consumer = new ConsumingAgent(TOPIC_NAME);
		consumer.declareAccessPurpose(new AccessPurposeDeclaration(TOPIC_NAME, "billing"));

		final var producer = new ProducingAgent(TOPIC_NAME);
		//producer.reserveIntendedPurpose(new IntendedPurposeDeclaration("user-1234", ));

		consumer.consume();
		producer.produce();

	}

	class ProducingAgent {
		private final Properties producerConfig;
		private final String topicName;

		public ProducingAgent(String topicName) {
			this.topicName = topicName;

			final var producerConfig = new Properties();
			producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testUtils.getProxyHost(compose));
			this.producerConfig = producerConfig;
		}

		public void reserveIntendedPurpose(IntendedPurposeReservation reservation) {
			try (final var producer = new KafkaProducer<String, IntendedPurposeReservation>(producerConfig,
					new JsonSerializer<>(),
					new JsonSerializer<>())) {
				producer.send(new ProducerRecord<>("ip-reservations", reservation));
			}
		}

		public Thread produce() {
			return new Thread(() -> {
				try (final var producer = new KafkaProducer<String, MessageForDemo>(producerConfig,
						new JsonSerializer<>(),
						new JsonSerializer<>())) {
					Stream.generate(Date::new)
							.forEach(date -> {
								producer.send(new ProducerRecord<>(topicName, new MessageForDemo("user-1234", "DE",
										date.getTime())));
								try {
									Thread.sleep(1000);
								} catch (InterruptedException e) {
									throw new RuntimeException(e);
								}
							});
				}
			});
		}
	}

	class ConsumingAgent {
		private final Properties consumerConfig, producerConfig;
		private final String topicName;

		public ConsumingAgent(String topicName) {
			this.topicName = topicName;

			final var agentId = "consumer-" + UUID.randomUUID();

			final var producerConfig = new Properties();
			producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testUtils.getProxyHost(compose));
			producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, agentId);
			this.producerConfig = producerConfig;

			final var consumerConfig = new Properties();
			consumerConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, testUtils.getProxyHost(compose));
			consumerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, agentId);
			this.consumerConfig = consumerConfig;
		}

		public void declareAccessPurpose(AccessPurposeDeclaration declaration) {
			try (final var producer = new KafkaProducer<String, AccessPurposeDeclaration>(producerConfig,
					new JsonSerializer<>(),
					new JsonSerializer<>())) {
				producer.send(new ProducerRecord<>("ap-declarations", declaration));
			}
		}

		public void consume(ForeachAction<String, MessageForDemo> onMessage) {
			final var builder = new StreamsBuilder();

			builder.stream(topicName, Consumed.with(Serdes.String(), new JsonSerde<>(MessageForDemo.class)))
					.peek(onMessage);

			final var topology = builder.build();

			new KafkaStreams(topology, consumerConfig).start();
		}
	}

}
