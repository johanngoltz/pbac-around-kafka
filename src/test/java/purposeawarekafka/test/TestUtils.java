package purposeawarekafka.test;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.testcontainers.containers.DockerComposeContainer;
import purposeawarekafka.pbac.model.MessageForDemo;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;

public class TestUtils {
	public TestUtils() {}

	Uuid doCreateTopic(NewTopic newTopic, AdminClient adminClient) throws InterruptedException, ExecutionException,
			TimeoutException {
		final var createTopics = adminClient.createTopics(List.of(newTopic));

		return createTopics.topicId(newTopic.name()).get(10, SECONDS);
	}

	AdminClient createAdminClient() {
		final var adminClientConfig = new Properties();
		adminClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");

		final var adminClient = KafkaAdminClient.create(adminClientConfig);
		return adminClient;
	}

	String getProxyHost(DockerComposeContainer compose) {
		return "%s:%d".formatted(
				compose.getServiceHost("pbac", 9093),
				compose.getServicePort("pbac", 9093));
	}

	KafkaStreams consume(DockerComposeContainer compose, String topicName,
	                     ForeachAction<String, MessageForDemo> onMessage) {
		final var props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, getProxyHost(compose));
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "integrationConsumer");

		final var builder = new StreamsBuilder();
		builder.stream(topicName, Consumed.with(Serdes.String(), new JsonSerde<>(MessageForDemo.class)))
				.peek(onMessage);

		final var topology = builder.build();

		return new KafkaStreams(topology, props);
	}

	@NotNull Thread produce(DockerComposeContainer compose, String topicName, Stream<MessageForDemo> messages,
	                        List<Instant> sendInstants) {
		return new Thread(() -> {
			final Properties producerConfig = getProducerConfig(compose);

			try (final var producer = new KafkaProducer<String, MessageForDemo>(producerConfig,
					new JsonSerializer<>(),
					new JsonSerializer<>())) {

				if (sendInstants == null)
					messages.forEach(body -> {
						producer.send(new ProducerRecord<>(topicName, body));
					});
				else
					messages.forEach(body -> {
						producer.send(new ProducerRecord<>(topicName, body));
						sendInstants.add(Instant.now());
					});
				producer.flush();
			}
		});
	}

	@NotNull
	private Properties getProducerConfig(DockerComposeContainer compose) {
		final var producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getProxyHost(compose));
		producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "the-client");
		return producerConfig;
	}
}