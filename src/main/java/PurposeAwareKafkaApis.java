import kafka.coordinator.group.GroupCoordinator;
import kafka.coordinator.transaction.TransactionCoordinator;
import kafka.network.RequestChannel;
import kafka.server.*;
import kafka.server.metadata.ConfigRepository;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.EnvelopeResponse;
import org.apache.kafka.common.utils.Time;
import scala.Option;

import java.util.Collection;
import java.util.List;

public class PurposeAwareKafkaApis extends KafkaApis {
    public PurposeAwareKafkaApis(RequestChannel requestChannel, MetadataSupport metadataSupport,
                                 ReplicaManager replicaManager,
                                 GroupCoordinator groupCoordinator, TransactionCoordinator txnCoordinator,
                                 AutoTopicCreationManager autoTopicCreationManager, int brokerId,
                                 KafkaConfig config, ConfigRepository configRepository,
                                 MetadataCache metadataCache, Metrics metrics, Option authorizer,
                                 QuotaFactory.QuotaManagers quotas, FetchManager fetchManager,
                                 BrokerTopicStats brokerTopicStats, String clusterId, Time time,
                                 DelegationTokenManager tokenManager, ApiVersionManager apiVersionManager) {
        super(requestChannel, metadataSupport, replicaManager, groupCoordinator, txnCoordinator,
                autoTopicCreationManager, brokerId, config, configRepository, metadataCache, metrics, authorizer,
                quotas, fetchManager, brokerTopicStats, clusterId, time, tokenManager, apiVersionManager);
    }

    final Collection<ApiKeys> fetchRequestKeys = List.of(ApiKeys.FETCH, ApiKeys.FETCH_SNAPSHOT, ApiKeys.OFFSET_FETCH);

    @Override
    public void handle(RequestChannel.Request request, RequestLocal requestLocal) {
        if (hasRequestSpecialHandling(request)) {
            System.out.println("Handling specially");
            // forward request to real Kafka, on response filter
        } else {
            // else forward request and response
        }

        final var incomingPayload = request.buffer();
        final var xxx = new EnvelopeResponse(incomingPayload, Errors.NONE);
        final var requestSend = request.context().buildResponseSend(xxx);

        final var forwarding = new RequestChannel.SendResponse(request, requestSend, Option.empty(), Option.empty());

        requestChannel().sendResponse(forwarding);
    }

    private boolean hasRequestSpecialHandling(RequestChannel.Request request) {
        return fetchRequestKeys.contains(request.header().apiKey());
    }
}
