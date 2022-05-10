/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import _root_.kafka.utils.Logging
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.network.RequestChannel
import kafka.server.QuotaFactory.QuotaManagers
import kafka.server.metadata.ConfigRepository
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.message.UpdateMetadataResponseData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.ApiKeys.{METADATA, UPDATE_METADATA}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.RecordConversionStats
import org.apache.kafka.common.requests.{MetadataRequest, UpdateMetadataResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.authorizer.Authorizer
import purposeawarekafka.ProxyToBrokerChannelManager

import scala.collection.Map
import scala.language.postfixOps

class KafkaApis(val requestChannel: RequestChannel,
                val metadataSupport: MetadataSupport,
                val replicaManager: ReplicaManager,
                val groupCoordinator: GroupCoordinator,
                val txnCoordinator: TransactionCoordinator,
                val autoTopicCreationManager: AutoTopicCreationManager,
                val brokerId: Int,
                val config: KafkaConfig,
                val configRepository: ConfigRepository,
                val metadataCache: MetadataCache,
                val metrics: Metrics,
                val authorizer: Option[Authorizer],
                val quotas: QuotaManagers,
                val fetchManager: FetchManager,
                brokerTopicStats: BrokerTopicStats,
                val clusterId: String,
                time: Time,
                val tokenManager: DelegationTokenManager,
                val apiVersionManager: ApiVersionManager) extends ApiRequestHandler with Logging {

    type FetchResponseStats = Map[TopicPartition, RecordConversionStats]
    this.logIdent = "[KafkaApi-%d] ".format(brokerId)
    val configHelper = new ConfigHelper(metadataCache, config, configRepository)
    val authHelper = new AuthHelper(authorizer)
    val requestHelper = new RequestHandlerHelper(requestChannel, quotas, time)
    val aclApis = new AclApis(authHelper, authorizer, requestHelper, "broker", config)
    val configManager = new ConfigAdminManager(brokerId, config, configRepository)
    val forwarder = new ProxyToBrokerChannelManager(
        MetadataCacheControllerNodeProvider(config, metadataCache),
        time,
        metrics,
        config,
        "ChannelName",
        Option("Prefix"),
        config.requestTimeoutMs.longValue)
    forwarder.start()

    override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
        try {
            trace(s"Handling request:${request.requestDesc(true)} from connection ${request.context.connectionId};" +
              s"securityProtocol:${request.context.securityProtocol},principal:${request.context.principal}")

            if (!apiVersionManager.isApiEnabled(request.header.apiKey)) {
                // The socket server will reject APIs which are not exposed in this scope and close the connection
                // before handing them to the request handler, so this path should not be exercised in practice
                throw new IllegalStateException(s"API ${request.header.apiKey} is not enabled")
            }

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
            replicaManager.tryCompleteActions()
            // The local completion time may be set while processing the request. Only record it if it's unset.
            if (request.apiLocalCompleteTimeNanos < 0)
                request.apiLocalCompleteTimeNanos = time.nanoseconds
        }
    }

    private def forward(request: RequestChannel.Request): Unit = {
        request.header.apiKey match {
            case UPDATE_METADATA => requestHelper.sendResponseExemptThrottle(request, new UpdateMetadataResponse(new UpdateMetadataResponseData().setErrorCode(Errors.NONE.code)))
            case _ =>
                val newRequest = request.header.apiKey match {
                    case METADATA => new MetadataRequest.Builder(request.body[MetadataRequest].data)
                    case _ => throw new NotImplementedError(request.header.apiKey + " is not handled")
                }
                forwarder.sendRequest(newRequest, new ControllerRequestCompletionHandler {
                    override def onTimeout(): Unit = ???

                    override def onComplete(response: ClientResponse): Unit =
                        requestChannel.sendResponse(request, response.responseBody, Option.empty)
                })
        }
    }
}
