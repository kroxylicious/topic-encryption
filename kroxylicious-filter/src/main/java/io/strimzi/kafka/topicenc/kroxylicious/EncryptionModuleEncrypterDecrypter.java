package io.strimzi.kafka.topicenc.kroxylicious;

import io.strimzi.kafka.topicenc.EncryptionModule;
import io.strimzi.kafka.topicenc.policy.InMemoryPolicyRepository;
import io.strimzi.kafka.topicenc.policy.JsonPolicyLoader;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncryptionModuleEncrypterDecrypter implements EncrypterDecrypter {

    private static final Logger logger = LoggerFactory.getLogger(EncryptionModuleEncrypterDecrypter.class);
    private static final Map<EncryptionModuleConfiguration, EncryptionModuleEncrypterDecrypter> crypters = new HashMap<>();
    private static final FileWatcherService watcher = new FileWatcherService();
    private EncryptionModule encryptionModule;

    public static EncrypterDecrypter getOrCreateInstance(File kmsDefsFile, File policyFile, boolean reloadOnFileChange) {
        EncryptionModuleConfiguration config = new EncryptionModuleConfiguration(kmsDefsFile.toPath(), policyFile.toPath());
        return crypters.computeIfAbsent(config, encryptionModuleConfiguration -> new EncryptionModuleEncrypterDecrypter(config, watcher, reloadOnFileChange));
    }

    record EncryptionModuleConfiguration(Path kmsDefsPath, Path topicPoliciesPath) {
    }

    private EncryptionModuleEncrypterDecrypter(EncryptionModuleConfiguration configuration, FileWatcherService watcher, boolean reloadOnFileChange) {
        reload(configuration);
        if (reloadOnFileChange) {
            reloadOnFileChange(watcher, configuration.kmsDefsPath, configuration);
            reloadOnFileChange(watcher, configuration.topicPoliciesPath, configuration);
        }
    }

    private void reloadOnFileChange(FileWatcherService watcher, Path path, EncryptionModuleConfiguration configuration) {
        watcher.watchFileForCreateOrModify(path, kind -> {
            logger.info("file changed {}, reloading configuration", path);
            reload(configuration);
        }).thenAccept(watchId -> {
            logger.info("Registered file change watchers for {}", path);
        }).exceptionally(throwable -> {
            logger.error("Failed to registered file change watchers for {}", path, throwable);
            return null;
        });
    }

    private void reload(EncryptionModuleConfiguration configuration) {
        try {
            encryptionModule = load(configuration.kmsDefsPath.toFile(), configuration.topicPoliciesPath.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic policies", e);
        }
    }

    private EncryptionModule load(File kmsDefsFile, File policyFile) throws IOException {
        List<TopicPolicy> topicPolicies = JsonPolicyLoader.loadTopicPolicies(kmsDefsFile, policyFile);
        InMemoryPolicyRepository inMemoryPolicyRepository = new InMemoryPolicyRepository(topicPolicies);
        return new EncryptionModule(inMemoryPolicyRepository);
    }

    @Override
    public boolean encrypt(ProduceRequestData.TopicProduceData topicData) {
        try {
            return encryptionModule.encrypt(topicData);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean decrypt(FetchResponseData.FetchableTopicResponse fetchRsp) {
        try {
            return encryptionModule.decrypt(fetchRsp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
