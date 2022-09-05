package purposeawarekafka.pbac.model;

public record AccessPurposeDeclaration(String topicName, String accessPurpose, String clientId) {
	public AccessPurposeDeclarationKey keyForPublish() {
		return new AccessPurposeDeclarationKey(topicName, clientId);
	}

	public AccessPurposeDeclarationValue valueForPublish() {
		return new AccessPurposeDeclarationValue(accessPurpose);
	}

	public static AccessPurposeDeclaration fromKeyValue(AccessPurposeDeclarationKey key,
	                                                    AccessPurposeDeclarationValue value) {
		return new AccessPurposeDeclaration(key.topic(), value.accessPurpose(), key.clientId());
	}
}
