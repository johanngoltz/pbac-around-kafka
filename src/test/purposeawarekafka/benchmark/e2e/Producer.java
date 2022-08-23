package purposeawarekafka.benchmark.e2e;

import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.support.serializer.JsonSerializer;
import purposeawarekafka.IntendedPurposeReservation;
import purposeawarekafka.IntendedPurposeReservationKey;
import purposeawarekafka.IntendedPurposeReservationValue;

import java.io.File;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class Producer implements Runnable {
	private final KafkaProducer<String, String> appProducer;
	private final KafkaProducer<IntendedPurposeReservationKey, IntendedPurposeReservationValue> ipProducer;

	private final List<String> userIds = Stream.generate(() -> "user-" + UUID.randomUUID()).limit(5).toList();

	private final Map<String, IntendedPurposeReservation> reservations;

	private final Map<String, String> messageTemplates;

	private final Random random = new Random();
	private final Uuid topicId;
	private final String topicName;
	private final Set<String> purposes;
	private final String benchmarkAgentId;
	private final SortedMap<Long, IntendedPurposeReservation> _ipPublished =
			Collections.synchronizedSortedMap(new TreeMap<>());
	public SortedMap<Long, IntendedPurposeReservation> ipPublished = Collections.unmodifiableSortedMap(_ipPublished);
	private final List<MessagePublished> messagesPublished = new ArrayList<>();

	public final Map<UUID, Long> sendTimestamps = Collections.synchronizedMap(new HashMap<>());

	public static void main(String[] args) throws Exception {
		final var bootstrapHost = System.getenv("BENCH_KAFKA_HOST");
		final var bootstrapPort = System.getenv("BENCH_KAFKA_PORT");
		final var topic = System.getenv("BENCH_TOPIC");
		final var purposes = Set.copyOf(List.of(System.getenv("BENCH_PURPOSES").split(",")));
		final var hostName = Optional.ofNullable(System.getenv("HOSTNAME")).orElse(System.getenv("COMPUTERNAME"));

		final var instance = new Producer("%s:%s".formatted(bootstrapHost, bootstrapPort), topic, purposes, hostName);

		final var t = new Thread(instance);
		try (final var controlSocket = new ServerSocket(8080)) {
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

	public Producer(String bootstrapServer, String topicName, Set<String> purposes, String benchmarkAgentId) throws ExecutionException, InterruptedException {
		this.topicName = topicName;
		this.purposes = purposes;
		this.benchmarkAgentId = benchmarkAgentId;

		assert purposes.size() == 2;

		final var producerConfig = new Properties();
		producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

		final var stringSerializer = new StringSerializer();
		this.appProducer = new KafkaProducer<>(producerConfig, stringSerializer, stringSerializer);

		this.ipProducer = new KafkaProducer<>(producerConfig, new JsonSerializer<>(), new JsonSerializer<>());

		messageTemplates = makeMessageTemplates();

		topicId = Util.getTopicId(bootstrapServer, topicName);

		reservations = new HashMap<>();
		for (final var userId : userIds) {
			reservations.put(userId, getInitialIntendedPurposeReservation(userId));
		}
		for (final var reservation : reservations.values()) {
			ipProducer.send(new ProducerRecord<>("ip-reservations", reservation.getKeyForPublish(),
					reservation.getValueForPublish()));
		}

	}

	@NotNull
	private Map<String, String> makeMessageTemplates() {
		final var result = new HashMap<String, String>();

		this.userIds.forEach(userId -> {
			final var almostJson = "{\"userId\":\"%s\", \"payload\":\"%%s\"}".formatted(userId);
			result.put(userId, almostJson);
		});

		return result;
	}

	public void run() {
		try {
			var previousReservation = 0L;
			while (!Thread.interrupted()) {
				if (System.currentTimeMillis() - previousReservation > 10_000) {
					reserveIntendedPurpose();
					previousReservation = System.currentTimeMillis();
				} else {
					publishMessage();
				}
			}
		} finally {
			reportResults();

			ipProducer.close(Duration.ZERO);
			appProducer.close(Duration.ZERO);
		}
	}

	@SneakyThrows
	private void reportResults() {
		File ipSentFile = new File("ipsent-%s.csv".formatted(benchmarkAgentId));
		try (final var pw = new PrintWriter(ipSentFile)) {
			pw.println("timestamp;allowed;prohibited");
			for (final var intendedPurposePublished : new HashMap<>(_ipPublished).entrySet()) {
				pw.print(intendedPurposePublished.getKey());
				pw.print(';');
				pw.print(String.join(",", intendedPurposePublished.getValue().allowed()));
				pw.print(';');
				pw.print(String.join(",", intendedPurposePublished.getValue().prohibited()));
				pw.println();
			}
		}

		final var messagesSentFile = new File("msgsent-%s.csv".formatted(benchmarkAgentId));
		try (final var pw = new PrintWriter(messagesSentFile)) {
			pw.println("timestamp");
			for (final var messagePublished : List.copyOf(messagesPublished)) {
				pw.print(messagePublished.timestamp());
				pw.println();
			}
		}
	}

	private void publishMessage() {
		final var anyUserId = randomUserId();
		final var msgUuid = UUID.randomUUID();
		final var message = messageTemplates.get(anyUserId).formatted(msgUuid);
		final var record = new ProducerRecord<String, String>(topicName, message);

		appProducer.send(record, (metadata, exception) -> {
			sendTimestamps.put(msgUuid, metadata.timestamp());
			messagesPublished.add(new MessagePublished(message, metadata.timestamp()));
		});
	}

	private String randomUserId() {
		return userIds.get(random.nextInt(userIds.size()));
	}

	private void reserveIntendedPurpose() {
		final var anyUserId = randomUserId();

		final var reservation = requireNonNull(reservations.get(anyUserId));

		// Make an AIP PIP, or vice-versa; in-place
		if (reservation.prohibited().size() == 0 || reservation.allowed().size() > 0 && random.nextBoolean())
			movePurpose(reservation.allowed(), reservation.prohibited());
		else movePurpose(reservation.prohibited(), reservation.allowed());

		final var record = new ProducerRecord<>("ip-reservations", reservation.getKeyForPublish(),
				reservation.getValueForPublish());
		ipProducer.send(record, (metadata, exception) -> _ipPublished.put(metadata.timestamp(), reservation));
	}

	private void movePurpose(Set<String> moveFrom, Set<String> moveTo) {
		assert !moveFrom.isEmpty();

		final var source = moveFrom.toArray();
		final var randomPurpose = (String) source[random.nextInt(source.length)];
		moveTo.add(randomPurpose);
		moveFrom.remove(randomPurpose);
	}

	@NotNull
	private IntendedPurposeReservation getInitialIntendedPurposeReservation(String userId) {
		return new IntendedPurposeReservation(userId, ".userId", topicId.toString(), new HashSet<>(purposes),
				new HashSet<>());
	}

	record MessagePublished(String message, long timestamp) {}
}
