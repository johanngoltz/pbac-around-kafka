package purposeawarekafka;

import kafka.Kafka;
import kafka.security.CredentialProvider;
import kafka.server.KafkaConfig;
import kafka.server.KafkaRequestHandlerPool;
import kafka.server.MetadataCache;
import kafka.server.SimpleApiVersionManager;
import lombok.SneakyThrows;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import scala.Option;
import scala.collection.Map$;

import java.util.concurrent.CompletableFuture;

public class PurposeAwareKafka {

	private static final Time time = Time.SYSTEM;

	@SneakyThrows
	public static void main(String[] args) {
		final var purposeStore = new PurposeStore("localhost:9093");
		final var purposes = new Purposes(purposeStore);
		new Thread(purposeStore).start();

		final var properties = Kafka.getPropsFromArgs(args);
		final var config = KafkaConfig.fromProps(properties, false);

		final var metrics = kafka.server.Server.initializeMetrics(config, time, "casjkj");

		final var tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames());
		final var credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames(), tokenCache);

		final var apiVersionManager = new SimpleApiVersionManager(ApiMessageType.ListenerType.ZK_BROKER);

		final var socketServer = new kafka.network.SocketServer(config, metrics, time, credentialProvider,
				apiVersionManager);


		socketServer.startup(false, Option.empty(), config.dataPlaneListeners());

		final var requestHandlerPool = new KafkaRequestHandlerPool(-1,
				socketServer.dataPlaneRequestChannel(),
				new ForwardingApiRequestHandler(socketServer.dataPlaneRequestChannel(),
						config,
						time,
						metrics,
						MetadataCache.zkMetadataCache(config.brokerId()),
						purposes),
				time,
				1,
				"metricNameAvgIdle",
				"handler");

		socketServer.startProcessingRequests((scala.collection.Map<Endpoint, CompletableFuture<Void>>) Map$.MODULE$.empty());

		System.in.read();
	}
}
