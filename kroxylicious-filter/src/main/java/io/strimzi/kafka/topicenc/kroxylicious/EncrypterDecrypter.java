package io.strimzi.kafka.topicenc.kroxylicious;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;

public interface EncrypterDecrypter {
    public boolean encrypt(ProduceRequestData.TopicProduceData topicData);

    public boolean decrypt(FetchResponseData.FetchableTopicResponse fetchRsp);
}
