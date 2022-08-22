package purposeawarekafka.test;

import org.junit.Assert;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.junit.Test;
import purposeawarekafka.IntendedPurposeReservationKey;

import java.nio.charset.StandardCharsets;

public class KeySerializationTest {
	@Test
	public void test() throws Exception {
		final var key1 = new IntendedPurposeReservationKey("some-user", ".userId", "the-topic");
		final var result1 = new JsonSerializer<IntendedPurposeReservationKey>().serialize("ip-reservations", key1);

		final var expected1 = "{\"topic\":\"the-topic\",\"userIdExtractor\":\".userId\",\"userId\":\"some-user\"}";
		Assert.assertEquals(expected1, new String(result1, StandardCharsets.UTF_8));

		final var key2 = new IntendedPurposeReservationKey("some-user", ".userId", "the-topic-2");
		final var result2 = new JsonSerializer<IntendedPurposeReservationKey>().serialize("ip-reservations", key2);

		for (int i = 0; i < result1.length; i++) {
			if (result1[i] != result2[i]) {
				Assert.assertTrue(result1[i] < result2[i]);
				break;
			}
		}
	}
}
