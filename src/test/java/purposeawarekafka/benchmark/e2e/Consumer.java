package purposeawarekafka.benchmark.e2e;

import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.springframework.kafka.support.serializer.JsonSerializer;
import purposeawarekafka.pbac.model.AccessPurposeDeclaration;
import purposeawarekafka.pbac.model.AccessPurposeDeclarationKey;
import purposeawarekafka.pbac.model.AccessPurposeDeclarationValue;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public class Consumer implements Runnable {
	public final String purpose;
	private final String benchmarkAgentId;
	private final Uuid topicId;
	private final KafkaStreams streams;
	private final List<MessageReceived> messagesReceived = Collections.synchronizedList(new ArrayList<>());
	public final Queue<Triple<UUID, Long, Boolean>> receiveTimestamps = new LinkedBlockingQueue<>();
	private String bootstrapServer;

	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
		final var bootstrapHost = System.getenv("BENCH_KAFKA_HOST");
		final var bootstrapPort = System.getenv("BENCH_KAFKA_PORT");
		final var topic = System.getenv("BENCH_TOPIC");
		final var purpose = System.getenv("BENCH_PURPOSE");
		assert !purpose.contains(",");
		final var hostName = Optional.ofNullable(System.getenv("HOSTNAME")).orElse(System.getenv("COMPUTERNAME"));

		final var instance = new Consumer("%s:%s".formatted(bootstrapHost, bootstrapPort), topic, purpose, hostName);

		final var t = new Thread(instance);
		try (final var controlSocket = new ServerSocket(8081)) {
			System.out.println("Waiting for START command");
			controlSocket.accept().close();

			System.out.println("Benchmark starting");
			t.start();

			controlSocket.accept().close();
		} finally {
			System.out.println("Benchmark stopping");
			t.interrupt();
			t.join();
			System.out.println("Benchmark stopped.");
		}
	}

	@SneakyThrows
	public Consumer(String bootstrapServer, String topicName, String purpose, String benchmarkAgentId) {
		this.bootstrapServer = bootstrapServer;
		this.topicId = Util.getTopicId(bootstrapServer, topicName);
		this.purpose = purpose;
		this.benchmarkAgentId = benchmarkAgentId;

		final var consumerConfig = new Properties();
		consumerConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		consumerConfig.put(StreamsConfig.CLIENT_ID_CONFIG, benchmarkAgentId);
		consumerConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "benchmark-app-" + UUID.randomUUID());
		consumerConfig.put(ConsumerConfig.CHECK_CRCS_CONFIG, "false");


		final var builder = new StreamsBuilder();

		builder.stream(topicName, Consumed.with(Serdes.UUID(), Serdes.String())).peek((key, value) -> {
			// messagesReceived.add(new MessageReceived(System.currentTimeMillis(), null));
			final var wasFiltered = value.startsWith("-");
			receiveTimestamps.add(Triple.of(key, System.currentTimeMillis(), wasFiltered));
		});

		final var topology = builder.build();

		this.streams = new KafkaStreams(topology, consumerConfig);
	}

	public void run() {
		try {
			declareAp(bootstrapServer);
			streams.start();
			Thread.currentThread().join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			// reportResults();
			streams.close();
		}
	}

	@SneakyThrows
	private void reportResults() {
		File ipSentFile = new File("msgreceived-%s.csv".formatted(benchmarkAgentId));
		try (final var pw = new PrintWriter(ipSentFile)) {
			pw.println("timestamp");
			for (MessageReceived messageReceived : List.copyOf(messagesReceived)) {
				pw.print(messageReceived.timestamp());
				pw.println();
			}
		}
	}

	private void declareAp(String bootstrapServer) {
		final var producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

		final var declaration = new AccessPurposeDeclaration(topicId.toString(), purpose, benchmarkAgentId);
		try (final var apProducer =
				     new KafkaProducer<AccessPurposeDeclarationKey, AccessPurposeDeclarationValue>(producerConfig,
						     new JsonSerializer<>(), new JsonSerializer<>())) {
			apProducer.send(new ProducerRecord<>("ap-declarations", declaration.keyForPublish(),
					declaration.valueForPublish()));
		}

	}

	record MessageReceived(long timestamp, String message) {}
}
