package purposeawarekafka

import kafka.network.RequestChannel
import kafka.server._
import kafka.utils.Logging
import lombok.extern.slf4j.Slf4j
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{ApiVersionsRequest, MetadataRequest}
import org.apache.kafka.common.utils.Time

class ForwardingApiRequestHandler(val requestChannel: RequestChannel, val config: KafkaConfig, time: Time, metrics: Metrics, metadataCache: MetadataCache) extends ApiRequestHandler with Logging {
    //private val log = org.slf4j.LoggerFactory.getLogger(classOf[ForwardingApiRequestHandler])

    val requestHelper = new RequestHandlerHelper(requestChannel, QuotaFactory.instantiate(config, metrics, time, "quotaprefix") , time)
    val forwarder = new ProxyToBrokerChannelManager(
        MetadataCacheControllerNodeProvider(config, metadataCache),
        time,
        metrics,
        config,
        "ChannelName",
        Option("Prefix"),
        config.requestTimeoutMs.longValue)

    def forward(request: RequestChannel.Request): Unit = {
        val newRequest = request.header.apiKey match {
            case ApiKeys.API_VERSIONS => new ApiVersionsRequest.Builder()
            case ApiKeys.METADATA => new MetadataRequest.Builder(request.body[MetadataRequest].data)
            case _ => throw new NotImplementedError(s"ApiKey ${request.header.apiKey} is not handled")
        }
        forwarder.sendRequest(newRequest, new ControllerRequestCompletionHandler {
            override def onTimeout(): Unit = ???

            override def onComplete(response: ClientResponse): Unit =
                requestChannel.sendResponse(request, response.responseBody, Option.empty)
        })
    }

    override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
        try {
            trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
              s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}", null)

            forward(request)
        } catch {
            case e: FatalExitError => throw e
            case e: Throwable =>
                error(s"Unexpected error handling request ${request.requestDesc(true)} " +
                  s"with context ${request.context}", e)
                requestHelper.handleError(request, e)
        } finally {
            // try to complete delayed action. In order to avoid conflicting locking, the actions to complete delayed requests
            // are kept in a queue. We add the logic to check the ReplicaManager queue at the end of KafkaApis.handle() and the
            // expiration thread for certain delayed operations (e.g. DelayedJoin)
            // The local completion time may be set while processing the request. Only record it if it's unset.
            if (request.apiLocalCompleteTimeNanos < 0)
                request.apiLocalCompleteTimeNanos = time.nanoseconds
        }
    }
}
