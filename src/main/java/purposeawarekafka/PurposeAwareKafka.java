package purposeawarekafka;

import kafka.Kafka;
import kafka.cluster.EndPoint;
import kafka.security.CredentialProvider;
import kafka.server.KafkaConfig;
import kafka.server.SimpleApiVersionManager;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import scala.Option;
import scala.collection.Seq;
import scala.collection.Seq$;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class PurposeAwareKafka {

	private static final Time time = Time.SYSTEM;

	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		final var purposeStore = new PurposeStore();
		new Thread(purposeStore).start();
		// new Thread(new Server(purposeStore)).start();

		final var properties = Kafka.getPropsFromArgs(args);
		final var config = KafkaConfig.fromProps(properties, false);

		final var metrics = kafka.server.Server.initializeMetrics(config, time, "casjkj");

		final var tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames());
		final var credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames(), tokenCache);

		final var apiVersionManager = new SimpleApiVersionManager(ApiMessageType.ListenerType.BROKER);

		final var socketServer = new kafka.network.SocketServer(config, metrics, time, credentialProvider,
				apiVersionManager);

		socketServer.startup(true, Option.empty(), config.dataPlaneListeners());

		System.in.read();
	}
}
