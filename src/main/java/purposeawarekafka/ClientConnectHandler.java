package purposeawarekafka;

import lombok.RequiredArgsConstructor;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

@RequiredArgsConstructor
public class ClientConnectHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {
	private final AsynchronousServerSocketChannel serverChannel;
	private final AsynchronousSocketChannel kafkaChannel;
	private final Purposes purposes;


	/**
	 * Invoked when an operation has completed.
	 *
	 * @param clientChannel The result of the I/O operation.
	 * @param attachment
	 */
	@Override
	public void completed(AsynchronousSocketChannel clientChannel, Void attachment) {
		System.out.println("Connected");
		if (serverChannel.isOpen()) serverChannel.accept(null, this);

		if (clientChannel != null && clientChannel.isOpen()) {
			final var buffer = ByteBuffer.allocate(4096);
			clientChannel.read(buffer, buffer, new ClientReceiveHandler(clientChannel, kafkaChannel, purposes));
		}
	}

	/**
	 * Invoked when an operation fails.
	 *
	 * @param exc        The exception to indicate why the I/O operation failed
	 * @param attachment
	 */
	@Override
	public void failed(Throwable exc, Void attachment) {

	}
}
