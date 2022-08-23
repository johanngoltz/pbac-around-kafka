package purposeawarekafka.benchmark.e2e;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.TopicCollection;
import org.apache.kafka.common.Uuid;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Util {
	static Uuid getTopicId(String bootstrapServer, String topicName) throws InterruptedException, ExecutionException {
		try (final var adminClient = KafkaAdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
				bootstrapServer))) {
			return adminClient.describeTopics(TopicCollection.ofTopicNames(Collections.singleton(topicName))).allTopicNames().get().get(topicName).topicId();
		}
	}
}
