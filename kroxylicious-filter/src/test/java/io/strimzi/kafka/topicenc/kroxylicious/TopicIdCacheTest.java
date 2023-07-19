package io.strimzi.kafka.topicenc.kroxylicious;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiMessage;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.kroxylicious.proxy.filter.KrpcFilterContext;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class TopicIdCacheTest {

    private static final Uuid UNKNOWN_TOPIC_ID = Uuid.randomUuid();
    private static final Uuid KNOWN_TOPIC_ID = Uuid.randomUuid();
    private static final Uuid PENDING_TOPIC_ID = Uuid.randomUuid();
    private static final String KNOWN_TOPIC_NAME = "TOPIC_WIBBLE";
    private static final String RESOLVED_TOPIC_NAME = "TOPIC_RESOLVED";
    private TopicIdCache topicIdCache;
    private AsyncLoadingCache<Uuid, String> underlyingCache;
    private KrpcFilterContext filterContext;
    private CompletableFuture<ApiMessage> pendingFuture;

    @BeforeEach
    void setUp() {
        underlyingCache = Caffeine.newBuilder().buildAsync((key, executor) -> null);
        underlyingCache.put(KNOWN_TOPIC_ID, CompletableFuture.completedFuture(KNOWN_TOPIC_NAME));
        underlyingCache.put(PENDING_TOPIC_ID, new CompletableFuture<>());
        topicIdCache = new TopicIdCache(underlyingCache);
        filterContext = mock(KrpcFilterContext.class);
        pendingFuture = new CompletableFuture<>();

        lenient().when(filterContext.sendRequest(anyShort(), any())).thenReturn(pendingFuture);
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

    @Test
    void shouldSendMetadataRequestToResolveTopicNames() {
        //Given

        //When
        topicIdCache.resolveTopicNames(filterContext, Set.of(UNKNOWN_TOPIC_ID));

        //Then
        verify(filterContext).sendRequest(anyShort(), any(MetadataRequestData.class));
    }

    @Test
    void shouldIncludeTopicId() {
        //Given

        //When
        topicIdCache.resolveTopicNames(filterContext, Set.of(UNKNOWN_TOPIC_ID));

        //Then
        verify(filterContext).sendRequest(anyShort(), Mockito.argThat(apiMessage -> {
            assertThat(apiMessage).isInstanceOf(MetadataRequestData.class);
            assertThat(((MetadataRequestData) apiMessage).topics()).anyMatch(metadataRequestTopic -> UNKNOWN_TOPIC_ID.equals(metadataRequestTopic.topicId()));
            return true;
        }));
    }

    @Test
    void shouldIncludeMultipleTopicId() {
        //Given

        //When
        topicIdCache.resolveTopicNames(filterContext, Set.of(UNKNOWN_TOPIC_ID, PENDING_TOPIC_ID));

        //Then
        verify(filterContext).sendRequest(anyShort(), Mockito.argThat(apiMessage -> {
            assertThat(apiMessage).isInstanceOf(MetadataRequestData.class);
            assertThat(((MetadataRequestData) apiMessage).topics())
                    .allMatch(
                            metadataRequestTopic -> UNKNOWN_TOPIC_ID.equals(metadataRequestTopic.topicId()) || PENDING_TOPIC_ID.equals(metadataRequestTopic.topicId()));
            return true;
        }));
    }

    @Test
    void shouldReturnPendingFutures() {
        //Given

        //When
        topicIdCache.resolveTopicNames(filterContext, Set.of(UNKNOWN_TOPIC_ID, PENDING_TOPIC_ID));

        //Then
        assertThat(topicIdCache.getTopicName(UNKNOWN_TOPIC_ID)).isNotNull().isNotDone();
        assertThat(topicIdCache.getTopicName(PENDING_TOPIC_ID)).isNotNull().isNotDone();
    }

    @Test
    void shouldCacheFutureForTopicId() {
        //Given
        topicIdCache.resolveTopicNames(filterContext, Set.of(UNKNOWN_TOPIC_ID));

        //When
        final CompletableFuture<String> actualFuture = topicIdCache.getTopicName(UNKNOWN_TOPIC_ID);

        //Then
        assertThat(actualFuture).isNotNull().isNotDone();
    }

    @Test
    void shouldCompleteFutureWhenMetadataResponseDelivered() {
        //Given
        topicIdCache.resolveTopicNames(filterContext, Set.of(UNKNOWN_TOPIC_ID));
        final MetadataResponseData.MetadataResponseTopic responseTopic = new MetadataResponseData.MetadataResponseTopic();
        responseTopic.setTopicId(UNKNOWN_TOPIC_ID).setName(RESOLVED_TOPIC_NAME);
        final MetadataResponseData.MetadataResponseTopicCollection metadataResponseTopics = new MetadataResponseData.MetadataResponseTopicCollection();
        metadataResponseTopics.add(responseTopic);

        //When
        pendingFuture.complete(new MetadataResponseData().setTopics(metadataResponseTopics));

        //Then
        final CompletableFuture<String> actualFuture = topicIdCache.getTopicName(UNKNOWN_TOPIC_ID);
        assertThat(actualFuture).isCompletedWithValue(RESOLVED_TOPIC_NAME);
    }
}