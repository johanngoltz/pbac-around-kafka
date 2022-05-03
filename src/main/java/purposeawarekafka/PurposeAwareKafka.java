package purposeawarekafka;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class PurposeAwareKafka {
	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		final var purposeStore = new PurposeStore();
		new Thread(purposeStore).start();
		new Thread(new Server(purposeStore)).start();
		System.in.read();
	}
}
