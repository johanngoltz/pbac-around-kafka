package purposeawarekafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

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

	private String getProxyHost() {
		return "%s:%d".formatted(
				compose.getServiceHost("pbac", 9093),
				compose.getServicePort("pbac", 9093));
	}
}
