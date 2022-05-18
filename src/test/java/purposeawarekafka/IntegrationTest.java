package purposeawarekafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.File;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
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

}
