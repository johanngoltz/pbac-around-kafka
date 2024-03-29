package purposeawarekafka.kafka.server

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.server.{BrokerToControllerQueueItem, KafkaConfig}
import org.apache.kafka.clients.{ClientResponse, KafkaClient}
import org.apache.kafka.common.Node
import org.apache.kafka.common.utils.Time

import java.util.concurrent.LinkedBlockingQueue

class ProxyToBrokerRequestThread(networkClient: KafkaClient,
                                 config: KafkaConfig,
                                 time: Time,
                                 threadName: String) extends InterBrokerSendThread(threadName, networkClient, config.requestTimeoutMs, time) {
    private val requestQueue = new LinkedBlockingQueue[BrokerToControllerQueueItem]()
    private val underlyingKafka = new Node(-1,
        System.getenv("PBAC_KAFKA_HOST"),
        System.getenv("PBAC_KAFKA_PORT").toInt)

     def enqueue(request: BrokerToControllerQueueItem): Unit = {
        requestQueue.add(request)
        wakeup()
    }

     def handleResponse(queueItem: BrokerToControllerQueueItem)(response: ClientResponse): Unit = {
        // todo refer to super()
        if (response.authenticationException == null && response.versionMismatch == null && !response.wasDisconnected) {
            queueItem.callback.onComplete(response)
        } else {
            error(s"Request ${queueItem.request} failed!")
        }
    }

    override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
        val requestIter = requestQueue.iterator()
        while (requestIter.hasNext) {
            val request = requestIter.next
            requestIter.remove()
            return Some(RequestAndCompletionHandler(
                time.milliseconds,
                underlyingKafka,
                request.request,
                handleResponse(request)))
        }
        None
    }

    override def doWork(): Unit = super.pollOnce(Long.MaxValue)
}
