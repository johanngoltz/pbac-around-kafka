package integration;

import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.DockerComposeContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import purposeawarekafka.Server;

import java.io.File;

public class IntegrationTest {
	@ClassRule
	public static DockerComposeContainer compose =
			new DockerComposeContainer(
					new File("docker-compose.yml"))
					.withExposedService("kafka", 9092);

	@Test
	public void test() throws Exception {
		/*final var purposeAwareKafka = new Thread(new Server());
		purposeAwareKafka.setUncaughtExceptionHandler((t, e) -> System.out.println(e));
		System.out.println("Starting Server");
		purposeAwareKafka.start();*/

		final var topic = new GenericContainer("docker.io/bitnami/kafka:3.1")
				.withCommand("/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic quickstart-events " +
						"--bootstrap-server host.docker.internal:9002")
				.withLogConsumer(o -> {
					final var frame = (OutputFrame) o;
					System.out.println(frame.getUtf8String());
				})
				.withAccessToHost(true);
		topic.start();
		System.in.read();
	}
}
