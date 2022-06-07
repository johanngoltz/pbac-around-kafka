package purposeawarekafka

import kafka.network.RequestChannel
import org.apache.kafka.common.requests.{AbstractRequest, RequestHeader}

class DingsBums(val originalRequest: RequestChannel.Request)
  extends AbstractRequest.Builder[AbstractRequest](originalRequest.header.apiKey) {
    val header: RequestHeader = originalRequest.header
    private val toReturn = originalRequest.body[AbstractRequest]
    // TODO remove hack to see effects on throughput: are all other requests also blocked for 10 seconds?
    /*
    toReturn match {
        case request: FetchRequest => request.data.setMaxWaitMs(1)
        case _ =>
    }*/

    override def build(version: Short): AbstractRequest = {
        // ignore requested apiVersion
        toReturn
    }

    override def toString = "IdentityReturner{" + "toReturn=" + toReturn + '}'
}