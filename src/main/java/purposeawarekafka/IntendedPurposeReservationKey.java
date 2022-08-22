package purposeawarekafka;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ "topic", "userIdExtractor", "userId" })
public record IntendedPurposeReservationKey(String userId, String userIdExtractor, String topic) {}
