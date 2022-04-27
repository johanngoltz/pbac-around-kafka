/*
Copyright 2012 Artem Stasuk

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.github.terma.javaniotcpproxy;

import com.github.terma.javaniotcpserver.TcpServerHandler;
import org.apache.kafka.common.network.PlaintextTransportLayer;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import purposeawarekafka.Purposes;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TcpProxyConnector implements TcpServerHandler {

	private final static Logger LOGGER = Logger.getAnonymousLogger();

	private final TcpProxyBuffer clientBuffer = new TcpProxyBuffer();
	private final TcpProxyBuffer serverBuffer = new TcpProxyBuffer();
	private final SocketChannel clientChannel;

	private final Map<Integer, RequestHeader> requestHeaders = new HashMap<>();
	private final Purposes purposes = new Purposes();

	private Selector selector;
	private SocketChannel serverChannel;
	private TcpProxyConfig config;

	public TcpProxyConnector(SocketChannel clientChannel, TcpProxyConfig config) {
		this.clientChannel = clientChannel;
		this.config = config;
	}

	public void readFromClient(SelectionKey key) throws IOException {
		serverBuffer.writeFrom(clientChannel);
		final var buffer = serverBuffer.getBuffer();
		final var position = buffer.position();
		final var requestHeader = parseAndPrintRequest(buffer);
		requestHeaders.put(requestHeader.correlationId(), requestHeader);
		buffer.position(position);
		if (serverBuffer.isReadyToRead()) register();
	}

	public void readFromServer(SelectionKey key) throws IOException {
		clientBuffer.writeFrom(serverChannel);
		if (clientBuffer.isReadyToRead()) register();
	}

	public void writeToClient(SelectionKey key) throws IOException {
		final var buffer = clientBuffer.getBuffer();
		final var position = buffer.position();
		//final var anyHeaderVersion = requestHeaders.values().stream().findAny().get().headerVersion();
		if (buffer.hasRemaining()) {
			buffer.position(position + 4);
			final var responseHeader = ResponseHeader.parse(buffer, (short) 2);

			final var requestHeader = requestHeaders.get(responseHeader.correlationId());
			buffer.position(position + 4);
			final var response = AbstractResponse.parseResponse(buffer, requestHeader);

			System.out.println("Kafka: " + response + "→ Client");

			buffer.position(position);

			if (purposes.isRequestPurposeRelevant(requestHeader)) {
				final var compliantResponse = purposes.makeResponsePurposeCompliant(response);
				final var bytesWritten =
						compliantResponse.toSend(responseHeader, requestHeader.apiVersion()).writeTo(new PlaintextTransportLayer(key));
				buffer.position((int) (buffer.position() + bytesWritten));

			} else {
				clientBuffer.writeTo(clientChannel);
			}
		} else {
			System.out.println("buffer exhausted");
			clientBuffer.writeTo(clientChannel);
		}
		if (clientBuffer.isReadyToWrite()) register();
	}


	private RequestHeader parseAndPrintRequest(ByteBuffer buffer) {
		buffer.position(buffer.position() + 4);
		final var reqHeader = RequestHeader.parse(buffer);
		final var request = AbstractRequest.parseRequest(reqHeader.apiKey(), reqHeader.apiVersion(), buffer);
		System.out.println("Client: " + request.request + " → Kafka");
		return reqHeader;
	}

	private static record ResponseHeaderAndBody(ResponseHeader header, AbstractResponse body) {}

	private ResponseHeaderAndBody parseAndPrintResponse(ByteBuffer buffer, RequestHeader reqHeader) {
		buffer.position(4);
		final var resHeader = ResponseHeader.parse(buffer, reqHeader.headerVersion());
		buffer.position(4);
		final var response = AbstractResponse.parseResponse(buffer, reqHeader);
		System.out.println("Kafka: " + response + "→ Client");
		return new ResponseHeaderAndBody(resHeader, response);
	}


	public void writeToServer(SelectionKey key) throws IOException {
		serverBuffer.writeTo(serverChannel);
		if (serverBuffer.isReadyToWrite()) register();
	}

	public void register() throws ClosedChannelException {
		// todo store state (ie, request header) via attachment (as member variable might also work, maybe?)
		int clientOps = 0;
		if (serverBuffer.isReadyToWrite()) clientOps |= SelectionKey.OP_READ;
		if (clientBuffer.isReadyToRead()) clientOps |= SelectionKey.OP_WRITE;
		clientChannel.register(selector, clientOps, this);

		int serverOps = 0;
		if (clientBuffer.isReadyToWrite()) serverOps |= SelectionKey.OP_READ;
		if (serverBuffer.isReadyToRead()) serverOps |= SelectionKey.OP_WRITE;
		serverChannel.register(selector, serverOps, this);
	}

	private static void closeQuietly(SocketChannel channel) {
		if (channel != null) {
			try {
				channel.close();
			} catch (IOException exception) {
				if (LOGGER.isLoggable(Level.WARNING))
					LOGGER.log(Level.WARNING, "Could not close channel properly.", exception);
			}
		}
	}

	@Override
	public void register(Selector selector) {
		this.selector = selector;

		try {
			clientChannel.configureBlocking(false);

			final InetSocketAddress socketAddress = new InetSocketAddress(
					config.getRemoteHost(), config.getRemotePort());
			serverChannel = SocketChannel.open();
			serverChannel.connect(socketAddress);
			serverChannel.configureBlocking(false);

			register();
		} catch (final IOException exception) {
			destroy();

			if (LOGGER.isLoggable(Level.WARNING))
				LOGGER.log(Level.WARNING, "Could not connect to "
						+ config.getRemoteHost() + ":" + config.getRemotePort(), exception);
		}
	}

	@Override
	public void process(final SelectionKey key) {
		try {
			if (key.channel() == clientChannel) {
				if (key.isValid() && key.isReadable()) readFromClient(key);
				if (key.isValid() && key.isWritable()) writeToClient(key);
			}

			if (key.channel() == serverChannel) {
				if (key.isValid() && key.isReadable()) readFromServer(key);
				if (key.isValid() && key.isWritable()) writeToServer(key);
			}
		} catch (final ClosedChannelException exception) {
			destroy();

			if (LOGGER.isLoggable(Level.INFO))
				LOGGER.log(Level.INFO, "Channel was closed by client or server.", exception);
		} catch (final IOException exception) {
			destroy();

			if (LOGGER.isLoggable(Level.WARNING))
				LOGGER.log(Level.WARNING, "Could not process.", exception);
		}
	}

	@Override
	public void destroy() {
		closeQuietly(clientChannel);
		closeQuietly(serverChannel);
	}

}
