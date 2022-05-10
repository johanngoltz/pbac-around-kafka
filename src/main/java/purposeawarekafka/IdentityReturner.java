package purposeawarekafka;

import org.apache.kafka.common.requests.AbstractRequest;

public class IdentityReturner<T extends AbstractRequest> extends AbstractRequest.Builder<T> {
	private final T toReturn;

	public IdentityReturner(T toReturn) {
		super(toReturn.apiKey());

		this.toReturn = toReturn;
	}

	@Override
	public T build(short version) {
		// ignore requested apiVersion
		return toReturn;
	}
}
