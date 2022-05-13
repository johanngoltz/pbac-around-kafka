package purposeawarekafka

import kafka.common.RequestAndCompletionHandler
import kafka.server._
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.Node
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}

import java.util.concurrent.LinkedBlockingQueue
import scala.jdk.CollectionConverters.MapHasAsJava

class ProxyToBrokerChannelManager(controllerNodeProvider: ControllerNodeProvider,
                                  time: Time,
                                  metrics: Metrics,
                                  config: KafkaConfig,
                                  channelName: String,
                                  threadNamePrefix: Option[String],
                                  retryTimeoutMs: Long)
  extends BrokerToControllerChannelManager with Logging {
    private val logContext = new LogContext(s"[ProxyToBrokerChannelManager broker=${config.brokerId} name=$channelName] ")
    private val manualMetadataUpdater = new ManualMetadataUpdater()
    private val apiVersions = new ApiVersions()
    private val currentNodeApiVersions = NodeApiVersions.create()
    private val requestThread = newRequestThread

    override def start(): Unit = requestThread.start()

    override def shutdown(): Unit = {
        requestThread.shutdown()
        info(s"Proxy to controller channel manager for $channelName shutdown")
    }

    override def controllerApiVersions(): Option[NodeApiVersions] = ???

    override def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest], callback: ControllerRequestCompletionHandler): Unit =
        requestThread.enqueue(BrokerToControllerQueueItem(time.milliseconds, request, callback))

    private def newRequestThread: BrokerToControllerRequestThread = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
            SecurityProtocol.PLAINTEXT,
            null,
            config,
            null,
            null,
            time,
            false,
            logContext
        )

        val selector = new Selector(
            NetworkReceive.UNLIMITED,
            Selector.NO_IDLE_TIMEOUT_MS,
            metrics,
            time,
            channelName,
            Map("BrokerId" -> config.brokerId.toString).asJava,
            false,
            channelBuilder,
            logContext
        )

        val networkClient = new NetworkClient(
            selector,
            manualMetadataUpdater,
            config.brokerId.toString,
            1,
            50,
            50,
            Selectable.USE_DEFAULT_BUFFER_SIZE,
            Selectable.USE_DEFAULT_BUFFER_SIZE,
            config.requestTimeoutMs,
            config.connectionSetupTimeoutMs,
            config.connectionSetupTimeoutMaxMs,
            time,
            false,
            apiVersions,
            logContext
        )

        val threadName = "Forwarder"

        new BrokerToControllerRequestThread(
            networkClient,
            manualMetadataUpdater,
            controllerNodeProvider,
            config,
            time,
            threadName,
            retryTimeoutMs) {
            private val requestQueue = new LinkedBlockingQueue[BrokerToControllerQueueItem]()

            override def enqueue(request: BrokerToControllerQueueItem): Unit = {
                requestQueue.add(request)
                wakeup()
            }

            override def handleResponse(queueItem: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
                // todo refer to super()
                if (response.authenticationException == null && response.versionMismatch == null && !response.wasDisconnected) {
                    queueItem.callback.onComplete(response)
                } else {
                    error(s"Request ${queueItem.request} failed!")
                }
            }

            override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
                // val currentTimeMs = time.milliseconds
                val requestIter = requestQueue.iterator()
                while (requestIter.hasNext) {
                    val request = requestIter.next
                    requestIter.remove()
                    return Some(RequestAndCompletionHandler(
                        time.milliseconds,
                        new Node(-1, "kafka", 9092),
                        request.request,
                        handleResponse(request)))
                }
                None
            }

            override def doWork(): Unit = super.pollOnce(Long.MaxValue)
        }
    }
}
