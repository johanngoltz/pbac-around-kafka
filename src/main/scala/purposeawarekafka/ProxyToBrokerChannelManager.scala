package purposeawarekafka

import kafka.server._
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ChannelBuilders, NetworkReceive, Selectable, Selector}
import org.apache.kafka.common.requests.AbstractRequest
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{KafkaThread, LogContext, Time}

import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Random

class ProxyToBrokerChannelManager(time: Time,
                                  metrics: Metrics,
                                  config: KafkaConfig,
                                  channelName: String,
                                  threadNamePrefix: Option[String])
  extends BrokerToControllerChannelManager with Logging {
    private val manualMetadataUpdater = new ManualMetadataUpdater()
    private val apiVersions = new ApiVersions()
    private val random = Random.javaRandomToRandom(new java.util.Random())

    private val numThreads = System.getenv("PBAC_NUM_THREADS").toInt
    private val requestThreads = new mutable.ArrayBuffer[ProxyToBrokerRequestThread](numThreads)
    (0 to numThreads).foreach(i => newRequestThread(s"${threadNamePrefix.getOrElse("XXX")}-$i"))

    override def start(): Unit = for(thread <- requestThreads) thread.start()

    override def shutdown(): Unit = {
        for(thread <- requestThreads) thread.shutdown()
        info(s"Proxy to controller channel manager for $channelName shutdown")
    }

    override def controllerApiVersions(): Option[NodeApiVersions] = ???

    override def sendRequest(request: AbstractRequest.Builder[_ <: AbstractRequest], callback: ControllerRequestCompletionHandler): Unit = {
        val clientId = request.asInstanceOf[DingsBums].header.clientId()
        val threadId = random.nextInt(requestThreads.size)
        requestThreads(threadId).enqueue(BrokerToControllerQueueItem(time.milliseconds, request, callback))
    }

    private def newRequestThread(id: String) = {
        val threadName = s"$threadNamePrefix-pbac-request-forwarder-$id"
        val logContext = new LogContext(s"[ProxyToBrokerRequestThread broker=${config.brokerId} name=$threadName] ")

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
            threadName,
            Map("BrokerId" -> config.brokerId.toString).asJava,
            false,
            channelBuilder,
            logContext
        )

        val inflightRequestsFromEnv = System.getenv("max.in.flight.requests.per.connection")
        val inflightRequests = if (inflightRequestsFromEnv == null) 100 else inflightRequestsFromEnv.toInt

        val networkClient = new NetworkClient(
            selector,
            manualMetadataUpdater,
            config.brokerId.toString,
            inflightRequests,
            50,
            50,
            Selectable.USE_DEFAULT_BUFFER_SIZE,
            Selectable.USE_DEFAULT_BUFFER_SIZE,
            config.requestTimeoutMs * 10,
            config.connectionSetupTimeoutMs,
            config.connectionSetupTimeoutMaxMs,
            time,
            false,
            apiVersions,
            logContext
        ) {
            override def newClientRequest(nodeId: String, requestBuilder: AbstractRequest.Builder[_], createdTimeMs: Long, expectResponse: Boolean, requestTimeoutMs: Int, callback: RequestCompletionHandler): ClientRequest = {
                val (originalCorrelationId, originalClientId) = requestBuilder match {
                    case bums: DingsBums => (bums.header.correlationId, bums.header.clientId)
                    case _ => throw new IllegalArgumentException("Can only be called with requestBuilder: " + classOf[DingsBums] + ", but got " + requestBuilder.getClass.getSimpleName)
                }
                new ClientRequest(nodeId, requestBuilder, originalCorrelationId, originalClientId, createdTimeMs, expectResponse, requestTimeoutMs, callback)
            }
        }

        val thread = new ProxyToBrokerRequestThread(
            networkClient,
            config,
            time,
            threadName)

        requestThreads += thread
    }
}
