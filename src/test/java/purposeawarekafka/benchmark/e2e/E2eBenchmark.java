package purposeawarekafka.benchmark.e2e;

import org.apache.kafka.common.errors.TopicExistsException;

import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class E2eBenchmark {
	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
		final var bootstrapHost = System.getenv("BENCH_KAFKA_HOST");
		final var bootstrapPort = System.getenv("BENCH_KAFKA_PORT");
		final var bootstrapServer = "%s:%s".formatted(bootstrapHost, bootstrapPort);
		final var purposes = Set.copyOf(List.of(System.getenv("BENCH_PURPOSES").split(",")));
		final var hostName = Optional.ofNullable(System.getenv("HOSTNAME")).orElse(System.getenv("COMPUTERNAME"));
		final var topic = System.getenv("BENCH_TOPIC");
		final var numDummyReservationsToMake = Integer.parseInt(System.getenv("BENCH_NUM_DUMMY_RESERVATIONS"));
		final var newReservationFrequency = Duration.ofSeconds(Integer.parseInt(System.getenv("BENCH_NEW_RESERVATION_FREQUENCY_SECONDS")));

		final var threads = new ArrayList<Thread>();
		try (final var controlSocket = new ServerSocket(8080)) {
			System.out.println("Waiting for START command");
			controlSocket.accept().close();

			try {
				Util.createTopic(bootstrapServer, topic);
			} catch (ExecutionException e) {
				if (!(e.getCause() instanceof TopicExistsException)) throw e;
			}

			final var producer = new Producer(bootstrapServer, topic, purposes, hostName, numDummyReservationsToMake, newReservationFrequency);
			final var consumers = purposes.stream().map(purpose -> new Consumer(bootstrapServer, topic, purpose,
					hostName + "-" + purpose)).toList();
			final var resultReporter = new Thread(new ResultReporter(hostName, producer, consumers));

			threads.add(resultReporter);
			threads.add(new Thread(producer));
			consumers.stream().map(Thread::new).forEach(threads::add);

			System.out.println("Benchmark starting");
			threads.forEach(Thread::start);

			controlSocket.accept().close();
		} finally {
			System.out.println("Benchmark stopping");
			threads.forEach(Thread::interrupt);
			for (Thread thread : threads) {
				thread.join();
			}
			System.out.println("Benchmark stopped.");
		}
	}
}
