package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProduceEncryptFilter implements ProduceRequestFilter {


    private static final Logger log = LoggerFactory.getLogger(ProduceEncryptFilter.class);

    private final EncrypterDecrypter module;

    public ProduceEncryptFilter(TopicEncryptionConfig config) {
        module = config.encrypterDecrypter();
    }

    @Override
    public void onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
        try {
            for (ProduceRequestData.TopicProduceData topicDatum : request.topicData()) {
                try {
                    module.encrypt(topicDatum);
                } catch (Exception e) {
                    log.error("Failed to encrypt a produceRequest for topic: " + topicDatum.name(), e);
                    throw new RuntimeException(e);
                }
            }
            context.forwardRequest(header, request);
        } catch (Exception e) {
            sendErrorProduceResponse(request, context);
        }
    }

    private static void sendErrorProduceResponse(ProduceRequestData request, KrpcFilterContext context) {
        ProduceResponseData response = new ProduceResponseData();
        for (ProduceRequestData.TopicProduceData topicDatum : request.topicData()) {
            ProduceResponseData.TopicProduceResponse topicResponse = new ProduceResponseData.TopicProduceResponse();
            topicResponse.setName(topicDatum.name());
            for (ProduceRequestData.PartitionProduceData partitionDatum : topicDatum.partitionData()) {
                ProduceResponseData.PartitionProduceResponse partitionProduceResponse = new ProduceResponseData.PartitionProduceResponse();
                partitionProduceResponse.setIndex(partitionDatum.index());
                partitionProduceResponse.setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message());
                partitionProduceResponse.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                topicResponse.partitionResponses().add(partitionProduceResponse);
            }
            response.responses().add(topicResponse);
        }
        context.forwardResponse(response);
    }
}
