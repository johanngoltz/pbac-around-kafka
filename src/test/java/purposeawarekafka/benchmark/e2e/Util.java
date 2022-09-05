package purposeawarekafka.benchmark.e2e;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Util {
	static Uuid getTopicId(String bootstrapServer, String topicName) throws InterruptedException, ExecutionException {
		try (final var adminClient = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServer))) {
			return adminClient.describeTopics(TopicCollection.ofTopicNames(Collections.singleton(topicName))).allTopicNames().get().get(topicName).topicId();
		}
	}

	static void createTopic(String bootstrapServer, String topicName) throws ExecutionException, InterruptedException {
		try (final var adminClient = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServer))) {
			if (!adminClient.listTopics().names().get().contains(topicName))
				adminClient.createTopics(Collections.singleton(new NewTopic(topicName, Optional.of(10),
						Optional.empty()))).all().get();
		}
	}
}
