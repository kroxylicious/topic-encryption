package io.strimzi.kafka.topicenc.kroxylicious;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import org.apache.kafka.common.Uuid;

import com.github.benmanes.caffeine.cache.AsyncCacheLoader;

public class ContextCacheLoader implements AsyncCacheLoader<Uuid, String> {

    @Override
    public CompletableFuture<? extends String> asyncLoad(Uuid key, Executor executor) throws Exception {
        return null;
    }
}
