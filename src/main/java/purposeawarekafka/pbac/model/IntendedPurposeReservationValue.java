package purposeawarekafka.pbac.model;

import java.util.Set;

public record IntendedPurposeReservationValue(Set<String> allowed, Set<String> prohibited) {}

