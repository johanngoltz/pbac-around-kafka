package purposeawarekafka.benchmark.e2e;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Orchestrator {
	public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
		final var bootstrapHost = System.getenv("BENCH_KAFKA_HOST");
		final var bootstrapPort = System.getenv("BENCH_KAFKA_PORT");
		final var topic = System.getenv("BENCH_TOPIC");
		final var purposes = Set.copyOf(List.of(System.getenv("BENCH_PURPOSES").split(",")));
		final var hostName = Optional.ofNullable(System.getenv("HOSTNAME")).orElse(System.getenv("COMPUTERNAME"));

		final var producer = new Producer("%s:%s".formatted(bootstrapHost, bootstrapPort), topic, purposes, hostName);
		final var consumers = purposes.stream().map(purpose -> new Consumer("%s:%s".formatted(bootstrapHost,
				bootstrapPort), topic, purpose, hostName)).toList();
		final var resultReporter = new Thread(new ResultReporter(hostName, producer, consumers));

		final var threads = new ArrayList<Thread>();
		threads.add(resultReporter);
		threads.add(new Thread(producer));
		consumers.stream().map(Thread::new).forEach(threads::add);

		try (final var controlSocket = new ServerSocket(8081)) {
			System.out.println("Waiting for START command");
			controlSocket.accept().close();

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

	static class ResultReporter implements Runnable {
		private final PrintWriter pw;
		private final Producer producer;
		private final List<Consumer> consumers;

		public ResultReporter(String hostName, Producer producer, List<Consumer> consumers) throws FileNotFoundException {
			this.producer = producer;
			this.consumers = consumers;

			File latenciesFile = new File("latencies-%s.csv".formatted(hostName));
			this.pw = new PrintWriter(latenciesFile);
			pw.println("latency;pbacAppliedCorrectly");
		}

		@Override
		public void run() {
			while (!Thread.interrupted()) {
				try {
					Thread.sleep(5_000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				var i = 0;
				for (Consumer consumer : consumers) {
					final var receiveds = consumer.receiveTimestamps.entrySet().iterator();
					while (receiveds.hasNext()) {
						final var received = receiveds.next();
						receiveds.remove();

						final var queuedForSend = producer.sendTimestamps.get(received.getKey());
						final var latency = received.getValue() - queuedForSend;
						pw.print(latency);
						pw.print(';');

						final var applicableReservationAtQueueTime = producer.ipPublished.get(producer.ipPublished.tailMap(queuedForSend).firstKey());
						final var shouldHaveReceivedMessage = applicableReservationAtQueueTime.allowed().contains(consumer.purpose);
						pw.print(shouldHaveReceivedMessage ? 1 : 0);
						pw.println();
					}
					i++;
				}
				System.out.printf("Wrote %d latency measurements%n", i);
			}
		}
	}
}
