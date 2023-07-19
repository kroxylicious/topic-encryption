package io.strimzi.kafka.topicenc.kroxylicious;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.requests.MetadataRequest;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.kroxylicious.proxy.filter.KrpcFilterContext;

public class TopicIdCache {
    private final AsyncCache<Uuid, String> topicNamesById;

    public TopicIdCache() {
        this(Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(10)).buildAsync());
    }

    TopicIdCache(AsyncCache<Uuid, String> topicNamesById) {
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
        final MetadataRequest.Builder builder = new MetadataRequest.Builder(List.copyOf(topicIdsToResolve));
        final MetadataRequest metadataRequest = builder.build(builder.latestAllowedVersion());
        topicIdsToResolve.forEach(uuid -> topicNamesById.put(uuid, new CompletableFuture<>()));
        context.<MetadataResponseData> sendRequest(metadataRequest.version(), metadataRequest.data())
                .whenComplete((metadataResponseData, throwable) -> {
                    if (throwable != null) {
                        //TODO something sensible
                    }
                    else {
                        metadataResponseData.topics()
                                .forEach(metadataResponseTopic -> Objects.requireNonNull(topicNamesById.getIfPresent(metadataResponseTopic.topicId()))
                                        .complete(metadataResponseTopic.name()));
                        //If we were to get null from getIfPresent it would imply we got a result for a topic we didn't expect
                    }
                });
    }

}
