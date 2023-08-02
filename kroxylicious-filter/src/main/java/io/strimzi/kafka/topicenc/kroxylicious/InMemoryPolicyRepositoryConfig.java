package io.strimzi.kafka.topicenc.kroxylicious;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.kroxylicious.proxy.config.BaseConfig;

import java.io.File;

public class InMemoryPolicyRepositoryConfig extends BaseConfig {

    public static final String KMS_DEFINITIONS_FILE_PROP_NAME = "kmsDefinitionsFile";
    public static final String TOPIC_POLICIES_FILE_PROP_NAME = "topicPoliciesFile";
    public static final String RELOAD_ON_FILE_CHANGE_PROP_NAME = "reloadOnFileChange";

    private final EncrypterDecrypter encrypterDecrypter;

    @JsonCreator
    public InMemoryPolicyRepositoryConfig(@JsonProperty(KMS_DEFINITIONS_FILE_PROP_NAME) String kmsDefinitionsFile,
                                          @JsonProperty(TOPIC_POLICIES_FILE_PROP_NAME) String topicPoliciesFile,
                                          @JsonProperty(value = RELOAD_ON_FILE_CHANGE_PROP_NAME, defaultValue = "false") boolean reloadOnFileChange) {
        File kmsDefsFile = new File(kmsDefinitionsFile);
        if (!kmsDefsFile.exists()) {
            throw new IllegalArgumentException(KMS_DEFINITIONS_FILE_PROP_NAME + " " + kmsDefinitionsFile + " does not exist");
        }
        File policyFile = new File(topicPoliciesFile);
        if (!policyFile.exists()) {
            throw new IllegalArgumentException(TOPIC_POLICIES_FILE_PROP_NAME + " " + policyFile + " does not exist");
        }
        try {
            encrypterDecrypter = EncryptionModuleEncrypterDecrypter.getOrCreateInstance(kmsDefsFile, policyFile, reloadOnFileChange);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic policies", e);
        }
    }

    public EncrypterDecrypter encrypterDecrypter() {
        return encrypterDecrypter;
    }
}
