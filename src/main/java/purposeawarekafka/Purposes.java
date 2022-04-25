package purposeawarekafka;

import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;

import java.util.Collection;
import java.util.List;

public class Purposes {

	private final Collection<ApiKeys> relevantApiKeys = List.of(ApiKeys.FETCH, ApiKeys.OFFSET_FETCH);

	boolean isRequestPurposeRelevant(RequestHeader requestHeader) {
		return relevantApiKeys.contains(requestHeader.apiKey());
	}

	AbstractResponse makeResponsePurposeCompliant(AbstractResponse response) {
		return response;
	}
}
