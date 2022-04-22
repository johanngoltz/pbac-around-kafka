import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.util.concurrent.ExecutionException;

public class PurposeAwareKafka {
	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		new Thread(new Server()).start();
		System.in.read();
	}
}
