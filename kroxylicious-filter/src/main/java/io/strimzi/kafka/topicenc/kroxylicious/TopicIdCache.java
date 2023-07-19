package io.strimzi.kafka.topicenc.kroxylicious;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

import org.apache.kafka.common.Uuid;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;

import io.kroxylicious.proxy.filter.KrpcFilterContext;

public class TopicIdCache {
    private final AsyncLoadingCache<Uuid, String> topicNamesById;

    public TopicIdCache() {
        this(Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(10)).buildAsync((key, executor) -> {
            //TODO something clever.
            return null;
        }));
    }

    TopicIdCache(AsyncLoadingCache<Uuid, String> topicNamesById) {
        this.topicNamesById = topicNamesById;
    }

    /**
     * Exposes a future to avoid multiple clients triggering metadata requests for the same topicId.
     * @param topicId to convert to a name
     * @return the Future which will be completed when the topic name is resolved or <code>null</code> if the topic is not known (and is not currently being resolved)
     */
    public CompletableFuture<String> getTopicName(Uuid topicId) {
        return topicNamesById.getIfPresent(topicId);
    }

    public boolean hasResolvedTopic(Uuid topicId) {
        final CompletableFuture<String> topicNameFuture = topicNamesById.getIfPresent(topicId);
        //Caffeine converts failed or cancelled futures to null internally, so we don't have to handle them explicitly
        return topicNameFuture != null && topicNameFuture.isDone();
    }

    public void resolveTopicNames(KrpcFilterContext context, Set<Uuid> topicIdsToResolve) {

    }
}
