package purposeawarekafka;

import lombok.SneakyThrows;
import org.apache.commons.io.input.CharSequenceReader;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.io.IOException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

public class Purposes {

	private final Collection<ApiKeys> relevantApiKeys = List.of(ApiKeys.FETCH); //ApiKeys.OFFSET_FETCH
	private final jq jq = new jq();
	private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

	public boolean isRequestPurposeRelevant(RequestHeader requestHeader) {
		return relevantApiKeys.contains(requestHeader.apiKey());
	}

	@SneakyThrows
	public AbstractResponse makeResponsePurposeCompliant(AbstractResponse response) {
		if (response.data() instanceof FetchResponseData fr) {
			for (final var rrrr : fr.responses()) {
				for (final var partition : rrrr.partitions()) {
					final var records = (MemoryRecords) partition.records();
					for (Record record : records.records()) {
						makeRecordCompliant(record);
					}
				}
			}
			return response;
		} else {
			throw new UnsupportedOperationException(response.getClass().getName() + " is not supported!");
		}
	}

	private void makeRecordCompliant(Record record) throws IOException {
		final var buffer = record.value();
		buffer.mark();
		final var json = decoder.decode(buffer);
		final var isCompliant = jq.evaluate(".!=5", new CharSequenceReader(json));
		if (!isCompliant) {
			buffer.reset();
			while (buffer.hasRemaining())
				buffer.put((byte) '-');
			buffer.rewind();
		}
	}
}
