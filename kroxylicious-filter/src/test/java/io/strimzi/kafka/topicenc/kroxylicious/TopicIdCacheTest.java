package io.strimzi.kafka.topicenc.kroxylicious;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.Uuid;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import static org.assertj.core.api.Assertions.assertThat;

class TopicIdCacheTest {

    private static final Uuid UNKNOWN_TOPIC_ID = Uuid.randomUuid();
    private static final Uuid KNOWN_TOPIC_ID = Uuid.randomUuid();
    private static final Uuid PENDING_TOPIC_ID = Uuid.randomUuid();
    private static final String KNOWN_TOPIC_NAME = "TOPIC_WIBBLE";
    private TopicIdCache topicIdCache;
    private AsyncLoadingCache<Uuid, String> underlyingCache;

    @BeforeEach
    void setUp() {
        underlyingCache = Caffeine.newBuilder().buildAsync((key, executor) -> null);
        underlyingCache.put(KNOWN_TOPIC_ID, CompletableFuture.completedFuture(KNOWN_TOPIC_NAME));
        underlyingCache.put(PENDING_TOPIC_ID, new CompletableFuture<>());
        topicIdCache = new TopicIdCache(underlyingCache);
    }

    @Test
    void shouldReturnFalseForUnknownTopicId() {
        //Given

        //When
        final boolean isResolved = topicIdCache.hasResolvedTopic(UNKNOWN_TOPIC_ID);

        //Then
        assertThat(isResolved).isFalse();
    }

    @Test
    void shouldReturnFalseForTopicIdWhichIsStillInProgress() {
        //Given

        //When
        final boolean isResolved = topicIdCache.hasResolvedTopic(PENDING_TOPIC_ID);

        //Then
        assertThat(isResolved).isFalse();
    }

    @Test
    void shouldReturnFalseForTopicIdWhichHasFailed() {
        //Given
        underlyingCache.put(PENDING_TOPIC_ID, CompletableFuture.failedFuture(new IllegalStateException("boom boom boom")));

        //When
        final boolean isResolved = topicIdCache.hasResolvedTopic(PENDING_TOPIC_ID);

        //Then
        assertThat(isResolved).isFalse();
    }

    @Test
    void shouldReturnFalseForTopicIdOfWhichResolutionWasCancelled() {
        //Given
        final CompletableFuture<String> cancelledFuture = new CompletableFuture<>();
        cancelledFuture.cancel(true);
        underlyingCache.put(PENDING_TOPIC_ID, cancelledFuture);

        //When
        final boolean isResolved = topicIdCache.hasResolvedTopic(PENDING_TOPIC_ID);

        //Then
        assertThat(isResolved).isFalse();
    }

    @Test
    void shouldReturnFalseForTopicIdOfWhichResolutionWasCancelledAfterCaching() {
        //Given
        final CompletableFuture<String> cancelledFuture = new CompletableFuture<>();
        underlyingCache.put(PENDING_TOPIC_ID, cancelledFuture);
        cancelledFuture.cancel(true);

        //When
        final boolean isResolved = topicIdCache.hasResolvedTopic(PENDING_TOPIC_ID);

        //Then
        assertThat(isResolved).isFalse();
    }

    @Test
    void shouldReturnTrueForKnownTopicId() {
        //Given

        //When
        final boolean isResolved = topicIdCache.hasResolvedTopic(KNOWN_TOPIC_ID);

        //Then
        assertThat(isResolved).isTrue();
    }

    @Test
    void shouldReturnNameFromGetForKnownTopic() {
        //Given

        //When
        final CompletableFuture<String> topicName = topicIdCache.getTopicName(KNOWN_TOPIC_ID);

        //Then
        Assertions.assertThat(topicName).isCompletedWithValue(KNOWN_TOPIC_NAME);
    }

    @Test
    void shouldReturnNullFromGetForUnresolvedTopic() {
        //Given

        //When
        final CompletableFuture<String> topicName = topicIdCache.getTopicName(UNKNOWN_TOPIC_ID);

        //Then
        Assertions.assertThat(topicName).isNull();
    }

}