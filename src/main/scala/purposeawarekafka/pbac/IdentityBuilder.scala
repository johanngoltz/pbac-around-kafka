package purposeawarekafka.pbac

import kafka.network.RequestChannel
import org.apache.kafka.common.requests.{AbstractRequest, RequestHeader}

class IdentityBuilder(val originalRequest: RequestChannel.Request)
  extends AbstractRequest.Builder[AbstractRequest](originalRequest.header.apiKey) {
    val header: RequestHeader = originalRequest.header
    private val toReturn = originalRequest.body[AbstractRequest]

    override def build(version: Short): AbstractRequest = {
        // ignore requested apiVersion
        toReturn
    }

    override def toString = "IdentityBuilder{" + "toReturn=" + toReturn + '}'
}