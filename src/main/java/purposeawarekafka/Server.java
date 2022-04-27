package purposeawarekafka;

import com.github.terma.javaniotcpproxy.StaticTcpProxyConfig;
import com.github.terma.javaniotcpproxy.TcpProxyConnector;
import com.github.terma.javaniotcpserver.TcpServer;
import com.github.terma.javaniotcpserver.TcpServerConfig;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Server implements Runnable {

	private final TcpServer tcpServer;

	public Server() throws IOException, ExecutionException, InterruptedException {
		final var config = new StaticTcpProxyConfig(9002, "localhost", 9092, 1);
		this.tcpServer = new TcpServer(new TcpServerConfig(
				9002,
				clientChannel -> new TcpProxyConnector(clientChannel, config),
				1));
	}

	@Override
	public void run() {
		tcpServer.start();
	}
}
