package purposeawarekafka;

import kafka.Kafka;
import kafka.cluster.EndPoint;
import kafka.network.RequestChannel;
import kafka.security.CredentialProvider;
import kafka.server.*;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache;
import org.apache.kafka.common.utils.Time;
import scala.NotImplementedError;
import scala.Option;
import scala.collection.Map$;
import scala.collection.Seq;
import scala.collection.Seq$;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class PurposeAwareKafka {

	private static final Time time = Time.SYSTEM;

	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		final var purposeStore = new PurposeStore();
		final var purposes = new Purposes(purposeStore);
		new Thread(purposeStore).start();

		// new Thread(new Server(purposeStore)).start();

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
