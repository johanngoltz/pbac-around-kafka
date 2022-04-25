package purposeawarekafka;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class PurposeAwareKafka {
	public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
		new Thread(new Server()).start();
		System.in.read();
	}
}
