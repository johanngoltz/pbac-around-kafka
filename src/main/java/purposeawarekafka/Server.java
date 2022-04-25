package purposeawarekafka;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.ExecutionException;

public class Server implements Runnable {
	private final AsynchronousServerSocketChannel serverChannel;
	private final AsynchronousSocketChannel kafkaChannel;

	public Server() throws IOException, ExecutionException, InterruptedException {
		this.serverChannel = AsynchronousServerSocketChannel
				.open()
				.bind(new InetSocketAddress("0.0.0.0", 9002));
		this.kafkaChannel = AsynchronousSocketChannel.open();
		kafkaChannel.connect(new InetSocketAddress("localhost", 9092)).get();
		System.out.println("Server instantiated");
	}

	@Override
	public void run() {
		serverChannel.accept(null, new ClientConnectHandler(serverChannel, kafkaChannel, new Purposes()));
		System.out.println("Server running");
	}
}
