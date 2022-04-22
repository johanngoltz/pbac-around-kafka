import kafka.api.Request;
import kafka.network.RequestChannel;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;

public class Server implements Runnable {
	private final AsynchronousServerSocketChannel serverChannel;

	public Server() throws IOException {
		this.serverChannel = AsynchronousServerSocketChannel
				.open()
				.bind(new InetSocketAddress("localhost", 9002));
	}

	@Override
	public void run() {
		serverChannel.accept(null, new CompletionHandler<>() {
			@Override
			public void completed(AsynchronousSocketChannel clientChannel, Object attachment) {
				if (serverChannel.isOpen()) serverChannel.accept(null, this);

				if (clientChannel != null && clientChannel.isOpen()) {
					final var buffer = ByteBuffer.allocate(4096);
					clientChannel.read(buffer, null, new CompletionHandler<>() {
						@Override
						public void completed(Integer bytesRead, Object attachment) {
							buffer.flip();
							buffer.position(4);
							final var reqHeader = RequestHeader.parse(buffer);
							final var request = AbstractRequest.parseRequest(reqHeader.apiKey(), reqHeader.apiVersion(), buffer);
							System.out.println("Client: " + request.request + " → Kafka");
							buffer.rewind();
							try {
								final var kafkaChannel = AsynchronousSocketChannel.open();
								kafkaChannel.connect(new InetSocketAddress("localhost", 9092)).get();
								kafkaChannel.write(buffer).get();
								buffer.clear();
								kafkaChannel.read(buffer).get();
								kafkaChannel.close();
								buffer.position(4);
								//final var repHeader = ResponseHeader.parse(buffer, reqHeader.headerVersion());
								final var response = AbstractResponse.parseResponse(buffer, reqHeader);
								System.out.println("Kafka: " + response + "→ Client");
								buffer.flip();
								clientChannel.write(buffer).get();
							} catch (InterruptedException e) {
								e.printStackTrace();
							} catch (ExecutionException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
						}

						@Override
						public void failed(Throwable exc, Object attachment) {
							exc.printStackTrace();
						}
					});
				}
			}

			@Override
			public void failed(Throwable exc, Object attachment) {
				exc.printStackTrace();
			}
		});
	}
}
