package io.strimzi.kafka.topicenc.kroxylicious;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.common.Uuid;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.strimzi.kafka.topicenc.policy.PolicyRepository;

import io.kroxylicious.proxy.config.BaseConfig;

public class TopicEncryptionConfig extends BaseConfig {

    public static final String IN_MEMORY_POLICY_REPOSITORY_PROP_NAME = "inMemoryPolicyRepository";
    private final InMemoryPolicyRepositoryConfig inMemoryPolicyRepository;

    @JsonIgnore
    private static final ConcurrentHashMap<String, TopicIdCache> virtualClusterToTopicUUIDToTopicNameCache = new ConcurrentHashMap<>();
    private final ContextCacheLoader contextCacheLoader = new ContextCacheLoader();

    @JsonCreator
    public TopicEncryptionConfig(@JsonProperty(value = IN_MEMORY_POLICY_REPOSITORY_PROP_NAME) InMemoryPolicyRepositoryConfig inMemoryPolicyRepository) {
        this.inMemoryPolicyRepository = inMemoryPolicyRepository;
        Objects.requireNonNull(inMemoryPolicyRepository, "Currently " + IN_MEMORY_POLICY_REPOSITORY_PROP_NAME
                + " configuration is required as it is the only PolicyRepository implementation");
    }

    public ContextCacheLoader getContextCacheLoader() {
        return contextCacheLoader;
    }

    public PolicyRepository getPolicyRepository() {
        return inMemoryPolicyRepository.getPolicyRepository();
    }

    public TopicIdCache getTopicUuidToNameCache() {
        return virtualClusterToTopicUUIDToTopicNameCache.computeIfAbsent("VIRTUAL_CLUSTER_ID", (key) -> new TopicIdCache());
    }

}
