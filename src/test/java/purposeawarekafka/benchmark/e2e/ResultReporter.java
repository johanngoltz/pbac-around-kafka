package purposeawarekafka.benchmark.e2e;

import lombok.SneakyThrows;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import purposeawarekafka.IntendedPurposeReservation;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static java.util.Comparator.*;

class ResultReporter implements Runnable {
	private final Producer producer;
	private final List<Consumer> consumers;
	private final BufferedWriter resultsWriter;

	@SneakyThrows
	public ResultReporter(String hostName, Producer producer, List<Consumer> consumers) throws FileNotFoundException {
		this.producer = producer;
		this.consumers = consumers;

		this.resultsWriter = Files.newBufferedWriter(Path.of("latencies-%s.csv".formatted(hostName)),
				StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
		resultsWriter.write("lastMessageOfBatchQueued;purpose;aggLatency;aggTimeSinceReservationPublishedPbacCorrect;" +
				"countPbacCorrect;" + "aggTimeSinceReservationPublishedPbacWrong;countPbacWrong;" +
				"maxTimeSinceReservationPublishedPbacWrong;p99Latency");
		resultsWriter.newLine();
	}

	@Override
	@SneakyThrows
	public void run() {
		try {
			while (!Thread.interrupted()) {
				try {
					Thread.sleep(5_000);

					for (Consumer consumer : consumers) {
						Triple<UUID, Long, Boolean> received;
						var i = 0;
						var countPbacCorrect = 0;
						var countPbacWrong = 0;
						var aggLatency = 0d;
						var aggTimeSinceReservationPublishedPbacCorrect = 0d;
						var aggTimeSinceReservationPublishedPbacWrong = 0d;
						var maxTimeSinceReservationPublishedPbacWrong = Double.MIN_VALUE;
						var allLatencies = new ArrayList<Long>();
						Long queuedForSend = null;
						while ((received = consumer.receiveTimestamps.poll()) != null) {
							final var sendTimeStampAndAccessCountdown =
									producer.sendTimestamps.get(received.getLeft());

							if (sendTimeStampAndAccessCountdown == null) {
								System.out.println("Consumer received a message the producer did not send" +
										". " + "Did you delete Kafka's data between benchmark runs?");
								continue;
							}


							queuedForSend = sendTimeStampAndAccessCountdown.getLeft();

							maybeEvictFromSendTimestamps(received, sendTimeStampAndAccessCountdown);


							final var latency = received.getMiddle() - queuedForSend;
							aggLatency += latency;
							allLatencies.add(latency);

							final var applicableReservationAtQueueTime =
									getApplicableReservationAtQueueTime(queuedForSend);
							if (applicableReservationAtQueueTime != null) {
								final var sinceApplicableReservationMs =
										queuedForSend - applicableReservationAtQueueTime.getKey();
								final var wasMessageAllowedByReservation =
										applicableReservationAtQueueTime.getValue().allowed().contains(consumer.purpose);
								final var wasMessageFiltered = received.getRight();

								if (wasMessageAllowedByReservation != wasMessageFiltered) {
									aggTimeSinceReservationPublishedPbacCorrect += sinceApplicableReservationMs;
									countPbacCorrect++;
								} else {
									aggTimeSinceReservationPublishedPbacWrong += sinceApplicableReservationMs;
									if (maxTimeSinceReservationPublishedPbacWrong <= sinceApplicableReservationMs)
										maxTimeSinceReservationPublishedPbacWrong = sinceApplicableReservationMs;
									countPbacWrong++;
								}
							}
							i++;
						}
						if (i > 0) {
							aggLatency /= i;
							aggTimeSinceReservationPublishedPbacCorrect /= countPbacCorrect;
							aggTimeSinceReservationPublishedPbacWrong /= countPbacWrong;
							final var iso8601ts = Instant.ofEpochMilli(queuedForSend).toString();
							final var p99Latency = getP99Latency(allLatencies);

							resultsWriter.write("%s;%s;%s;%s;%d;%s;%d;%s;%s".formatted(iso8601ts, consumer.purpose, aggLatency,
									aggTimeSinceReservationPublishedPbacCorrect, countPbacCorrect,
									aggTimeSinceReservationPublishedPbacWrong, countPbacWrong,
									maxTimeSinceReservationPublishedPbacWrong, p99Latency));
							resultsWriter.newLine();
							System.out.printf("Wrote %d latency measurements%n", i);
						}
					}

					resultsWriter.flush();
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			resultsWriter.close();
		}
	}

	private float getP99Latency(ArrayList<Long> allLatencies) {
		allLatencies.sort(comparingLong(o -> (long) o).reversed());
		final var numLargestLatenciesToConsider = allLatencies.size() / 100;
		var p99Latency = Float.NaN;
		if (numLargestLatenciesToConsider > 0) {
			p99Latency =
					(float) allLatencies.subList(0, numLargestLatenciesToConsider).stream().reduce(Long::sum).get() / numLargestLatenciesToConsider;
		}
		return p99Latency;
	}

	private void maybeEvictFromSendTimestamps(Triple<UUID, Long, Boolean> received, Pair<Long, Byte> x) {
		if (x.getValue() < 2) producer.sendTimestamps.remove(received.getLeft());
		else x.setValue((byte) (x.getValue() - 1));
	}

	private Map.Entry<Long, IntendedPurposeReservation> getApplicableReservationAtQueueTime(Long queuedForSend) {
		final var ipDeclaredBeforeQueueForSend = producer.ipPublished.headMap(queuedForSend);
		if (ipDeclaredBeforeQueueForSend.isEmpty()) return null;

		final var applicableReservationTs = ipDeclaredBeforeQueueForSend.lastKey();
		return Map.entry(applicableReservationTs, producer.ipPublished.get(applicableReservationTs));
	}
}
