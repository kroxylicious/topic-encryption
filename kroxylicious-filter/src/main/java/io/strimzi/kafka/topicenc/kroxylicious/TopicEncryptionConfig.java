package io.strimzi.kafka.topicenc.kroxylicious;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.strimzi.kafka.topicenc.policy.PolicyRepository;

import io.kroxylicious.proxy.config.BaseConfig;

public class TopicEncryptionConfig extends BaseConfig {

    public static final String IN_MEMORY_POLICY_REPOSITORY_PROP_NAME = "inMemoryPolicyRepository";
    private final InMemoryPolicyRepositoryConfig inMemoryPolicyRepository;

    @JsonIgnore
    private static final ConcurrentHashMap<String, TopicIdCache> virtualClusterToTopicUUIDToTopicNameCache = new ConcurrentHashMap<>();

    @JsonCreator
    public TopicEncryptionConfig(@JsonProperty(value = IN_MEMORY_POLICY_REPOSITORY_PROP_NAME) InMemoryPolicyRepositoryConfig inMemoryPolicyRepository) {
        this.inMemoryPolicyRepository = inMemoryPolicyRepository;
        Objects.requireNonNull(inMemoryPolicyRepository, "Currently " + IN_MEMORY_POLICY_REPOSITORY_PROP_NAME
                + " configuration is required as it is the only PolicyRepository implementation");
    }

    public PolicyRepository getPolicyRepository() {
        return inMemoryPolicyRepository.getPolicyRepository();
    }

    public TopicIdCache getTopicUuidToNameCache() {
        return virtualClusterToTopicUUIDToTopicNameCache.computeIfAbsent("VIRTUAL_CLUSTER_ID", (key) -> new TopicIdCache());
    }

}
