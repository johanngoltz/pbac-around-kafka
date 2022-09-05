package purposeawarekafka.pbac.model;

import java.util.Set;

public record IntendedPurposeReservation(String userId, String userIdExtractor, String topic, Set<String> allowed,
                                         Set<String> prohibited) {
	public IntendedPurposeReservationKey getKeyForPublish() {
		return new IntendedPurposeReservationKey(userId, userIdExtractor, topic);
	}

	public IntendedPurposeReservationValue getValueForPublish() {
		return new IntendedPurposeReservationValue(allowed, prohibited);
	}

	public static IntendedPurposeReservation fromKeyValue(IntendedPurposeReservationKey key,
	                                                      IntendedPurposeReservationValue value) {
		return new IntendedPurposeReservation(key.userId(), key.userIdExtractor(), key.topic(), value.allowed(),
				value.prohibited());
	}

	public boolean allowsPurpose(String purpose) {
		return allowed.contains(purpose) && !prohibited.contains(purpose);
	}
}