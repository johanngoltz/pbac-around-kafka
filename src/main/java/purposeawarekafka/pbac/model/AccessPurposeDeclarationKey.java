package purposeawarekafka.pbac.model;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"topic", "clientId"})
public record AccessPurposeDeclarationKey(String topic, String clientId) {}
