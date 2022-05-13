package purposeawarekafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;

import java.io.File;
import java.util.*;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

public class IntegrationTest {
	public static final String TOPIC_NAME = "quickstart-events";
	@ClassRule
	public static DockerComposeContainer compose =
			new DockerComposeContainer(
					new File("src/test/resources/docker-compose.yml"))
					.withExposedService("pbac", 9093);

	private final String PROXY_HOST = "host.docker.internal:9093";

	@Test
	public void createTopic() throws Exception {
		final var newTopicName = "integrationTest." + UUID.randomUUID();
		final var newTopic = new NewTopic(newTopicName, Optional.empty(), Optional.empty());

		final var adminClientConfig = new Properties();
		adminClientConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");

		final var adminClient = KafkaAdminClient.create(adminClientConfig);

		final var createTopics = adminClient.createTopics(List.of(newTopic));

		createTopics.all().get(10, SECONDS);

		final var existingTopics = adminClient.listTopics().names().get(10, SECONDS);

		assertTrue(existingTopics.contains(newTopicName));
	}
}
