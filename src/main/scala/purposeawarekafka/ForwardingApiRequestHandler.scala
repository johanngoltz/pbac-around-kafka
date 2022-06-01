package purposeawarekafka

import kafka.network.RequestChannel
import kafka.server._
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

class ForwardingApiRequestHandler(val requestChannel: RequestChannel, val config: KafkaConfig, time: Time, metrics: Metrics, metadataCache: MetadataCache, purposes: Purposes) extends ApiRequestHandler with Logging {
    val requestHelper = new RequestHandlerHelper(requestChannel, QuotaFactory.instantiate(config, metrics, time, "quotaprefix"), time)
    val forwarder = new ProxyToBrokerChannelManager(
        MetadataCacheControllerNodeProvider(config, metadataCache),
        time,
        metrics,
        config,
        "ChannelName",
        Option("Prefix"),
        config.requestTimeoutMs.longValue)
    forwarder.start()
    val clazz = if (System.getenv("PBAC_CFG_MODE").equals("NONE")) {
        (request: RequestChannel.Request) => new NoopHandler(request)
    } else {
        (request: RequestChannel.Request) => new FilteringHandler(request)
    }

    def forward(request: RequestChannel.Request): Unit = {
        val newRequest = new IdentityReturner(request.body[AbstractRequest])
        forwarder.sendRequest(newRequest, clazz(request))
    }

    class FilteringHandler(val request: RequestChannel.Request) extends ControllerRequestCompletionHandler {
        override def onTimeout(): Unit = ???

        override def onComplete(response: ClientResponse): Unit = {
            // info("Filtering")
            if (purposes.isRequestPurposeRelevant(request.header))
                purposes.makeResponsePurposeCompliant(request.header, response.responseBody)
            requestChannel.sendResponse(request, response.responseBody, Option.empty)
        }
    }

    class NoopHandler(val request: RequestChannel.Request) extends ControllerRequestCompletionHandler {
        override def onTimeout(): Unit = ???

        override def onComplete(response: ClientResponse): Unit = {
            // info("Just forwarding")
            requestChannel.sendResponse(request, response.responseBody, Option.empty)
        }
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
