package integration;

import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import reactor.core.publisher.Mono;

import java.io.File;
import java.time.Duration;

import static org.junit.Assert.assertTrue;

public class IntegrationTest {
	@ClassRule
	public static DockerComposeContainer compose =
			new DockerComposeContainer(
					new File("src/test/resources/docker-compose.yml"))
					.withExposedService("kafka", 9092);

	@Test
	public void test() throws Exception {
		// need to manually start the purpose-aware server

		final var success = Mono.<Boolean>create(monoSink -> {
			final var topic = new GenericContainer("docker.io/bitnami/kafka:3.1")
					.withCommand("/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic quickstart-events " +
							"--bootstrap-server host.docker.internal:9002")
					.withLogConsumer(o -> {
						final var newMessages = ((OutputFrame) o).getUtf8String();
						System.out.println(newMessages);
						if (newMessages.contains("Created"))
							monoSink.success(true);
						else if (newMessages.contains("ERROR"))
							monoSink.success(false);
					});
			topic.start();
		}).block(Duration.ofSeconds(10));

		assertTrue(success);
	}
}
