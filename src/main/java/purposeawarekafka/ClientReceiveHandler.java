package purposeawarekafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutionException;

@RequiredArgsConstructor
public class ClientReceiveHandler implements CompletionHandler<Integer, ByteBuffer> {
	private final AsynchronousSocketChannel clientChannel;
	private final AsynchronousSocketChannel kafkaChannel;
	private final Purposes purposes;

	/**
	 * Invoked when an operation has completed.
	 *
	 * @param bytesRead The result of the I/O operation.
	 * @param buffer
	 */
	@Override
	public void completed(Integer bytesRead, ByteBuffer buffer) {
		if (bytesRead >= 0) {
			buffer.flip();
			final var reqHeader = parseAndPrintRequest(buffer);
			buffer.rewind();
			try {
				synchronized (kafkaChannel) {
					kafkaChannel.write(buffer).get();
					buffer.clear();
					kafkaChannel.read(buffer).get();
				}

				final var originalResponse = parseAndPrintResponse(buffer, reqHeader);
				if (purposes.isRequestPurposeRelevant(reqHeader)) {
					final var compliantResponse = purposes.makeResponsePurposeCompliant(originalResponse.body);
					final var send = compliantResponse.toSend(originalResponse.header, reqHeader.apiVersion());
					// todo write to buffer
				}
				buffer.flip();
				clientChannel.write(buffer).get();
				buffer.clear();
				clientChannel.read(buffer, buffer, this);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (ExecutionException e) {
				e.printStackTrace();
			}
		}
	}

	private RequestHeader parseAndPrintRequest(ByteBuffer buffer) {
		buffer.position(4);
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

	/**
	 * Invoked when an operation fails.
	 *
	 * @param exc        The exception to indicate why the I/O operation failed
	 * @param attachment
	 */
	@Override
	public void failed(Throwable exc, ByteBuffer attachment) {

	}
}
