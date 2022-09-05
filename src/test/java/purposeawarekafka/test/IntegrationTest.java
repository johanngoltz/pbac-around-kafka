package purposeawarekafka.test;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import purposeawarekafka.pbac.model.*;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

public class IntegrationTest {
	@ClassRule
	public static DockerComposeContainer compose =
			new DockerComposeContainer(
					new File("src/test/resources/docker-compose.yml"))
					.withExposedService("pbac", 9093)
					.waitingFor("kafka", new LogMessageWaitStrategy().withRegEx(".*Recorded new controller.*"))
					.waitingFor("pbac",
							new LogMessageWaitStrategy().withRegEx(".*to RUNNING.*").withStartupTimeout(ofSeconds(180)));
	private final TestUtils testUtils = new TestUtils();

	/** Tests that message types PBAC should not be applied to are always forwarded.
	 * @throws Exception
	 */
	@Test
	public void createTopic() throws Exception {
		final var newTopicName = "integrationTest." + UUID.randomUUID();
		final var newTopic = new NewTopic(newTopicName, Optional.empty(), Optional.empty());

		final var adminClient = testUtils.createAdminClient();
		testUtils.doCreateTopic(newTopic, adminClient);

		final var existingTopics = adminClient.listTopics().names().get(10, SECONDS);

		assertTrue(existingTopics.contains(newTopicName));
	}

	/** Tests application of basic AIP / PIP with one producer and two consumers.
	 * In the first part, both consumers declare AP that in the AIP set reserved by the producer.
	 * In the second part, the producer reserves new IPs that exclude one of the consumers.
	 * @throws Exception
	 */
	@Test
	public void respectChangedIntendedPurposes() throws Exception {
		final var TOPIC_NAME = "respect-changed-intended-purposes";

		final var adminClient = testUtils.createAdminClient();
		final var topicId = testUtils.doCreateTopic(
				new NewTopic(TOPIC_NAME, Optional.empty(), Optional.empty()), adminClient);

		final var producer = new ProducingAgent(TOPIC_NAME);
		try (final var billingConsumer = new ConsumingAgent(TOPIC_NAME, "billing-consumer");
		     final var marketingConsumer = new ConsumingAgent(TOPIC_NAME, "marketing-consumer")) {

			final var billingLatch = new CountDownLatch(1);
			final var marketingLatch = new CountDownLatch(1);

			billingConsumer.declareAccessPurpose(new AccessPurposeDeclaration(TOPIC_NAME, "billing",
					billingConsumer.clientId));
			marketingConsumer.declareAccessPurpose(new AccessPurposeDeclaration(TOPIC_NAME, "marketing",
					marketingConsumer.clientId));
			producer.reserveIntendedPurpose(new IntendedPurposeReservation("user-1234", ".userId",
					topicId.toString(),
					Set.of("billing", "marketing"), Set.of()));

			billingConsumer.consume((key, value) -> {
				billingLatch.countDown();
			}).start();
			marketingConsumer.consume((key, value) -> {
				marketingLatch.countDown();
			}).start();

			final var productionThread = producer.produce();
			productionThread.start();
			try {
				assertTrue("Billing consumer should always receive messages.",
						billingLatch.await(200, SECONDS));
				assertTrue("Marketing consumer should receive messages while allowed by user.",
						marketingLatch.await(200, SECONDS));

				billingConsumer.close();
				marketingConsumer.close();
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				productionThread.interrupt();
			}
		}

		try (final var billingConsumer = new ConsumingAgent(TOPIC_NAME, "billing-consumer");
		     final var marketingConsumer = new ConsumingAgent(TOPIC_NAME, "marketing-consumer")) {
			final var billingLatch = new CountDownLatch(1);

			producer.reserveIntendedPurpose(new IntendedPurposeReservation("user-1234", ".userId",
					topicId.toString(),
					Set.of("billing"), Set.of("marketing")));

			billingConsumer.consume((key, value) -> {
				billingLatch.countDown();
				billingConsumer.close();
			}).start();
			marketingConsumer.consume((key, value) -> Assert.fail(("Marketing consumer should not receive messages " +
					"after it was forbidden, got %s:%s").formatted(key, value)));

			final var productionThread = producer.produce();
			productionThread.start();
			try {
				assertTrue("Billing consumer should always receive messages.",
						billingLatch.await(200, SECONDS));

				// Wait a little to make sure the record is really not received.
				Thread.sleep(1000);
			} catch (Throwable t) {
				t.printStackTrace();
			} finally {
				productionThread.interrupt();
			}

		}
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
			try (final var producer = new KafkaProducer<IntendedPurposeReservationKey,
					IntendedPurposeReservationValue>(producerConfig,
					new JsonSerializer<>(),
					new JsonSerializer<>())) {
				producer.send(new ProducerRecord<>("ip-reservations", reservation.getKeyForPublish(),
						reservation.getValueForPublish()));
				producer.flush();
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
								System.out.println("Producer sent a message");
								try {
									Thread.sleep(1000);
								} catch (InterruptedException ignored) {}
							});
				}
			});
		}
	}

	class ConsumingAgent implements AutoCloseable {
		private final Properties consumerConfig, producerConfig;
		private final String topicName;
		final String clientId;
		private KafkaStreams streams;

		public ConsumingAgent(String topicName, String clientId) {
			this.topicName = topicName;
			this.clientId = clientId;

			final var producerConfig = new Properties();
			producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, testUtils.getProxyHost(compose));
			producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, this.clientId);
			this.producerConfig = producerConfig;

			final var consumerConfig = new Properties();
			consumerConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, testUtils.getProxyHost(compose));
			consumerConfig.put(StreamsConfig.CLIENT_ID_CONFIG, this.clientId);
			consumerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "integrationtest-appId-" + UUID.randomUUID());
			consumerConfig.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");
			this.consumerConfig = consumerConfig;
		}

		public void declareAccessPurpose(AccessPurposeDeclaration declaration) {
			try (final var producer =
					     new KafkaProducer<AccessPurposeDeclarationKey, AccessPurposeDeclarationValue>(producerConfig,
							     new JsonSerializer<>(),
							     new JsonSerializer<>())) {
				producer.send(new ProducerRecord<>("ap-declarations", declaration.keyForPublish(),
						declaration.valueForPublish()));
			}
		}

		public ConsumingAgent consume(ForeachAction<String, String> onMessage) {
			final var builder = new StreamsBuilder();

			builder.stream(topicName, Consumed.with(Serdes.String(), Serdes.String()))
					.peek(onMessage);

			final var topology = builder.build();

			this.streams = new KafkaStreams(topology, consumerConfig);
			return this;
		}

		public void start() {
			streams.start();
		}

		public void close() {
			streams.close(Duration.ZERO);
		}
	}

}
