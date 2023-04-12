package com.uci.orchestrator.Consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.uci.utils.BotService;
import com.uci.utils.dto.Adapter;
import com.uci.utils.dto.Result;
import com.uci.utils.kafka.SimpleProducer;
import lombok.extern.slf4j.Slf4j;
import messagerosa.core.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import javax.xml.bind.JAXBException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

@Slf4j
@Component
public class CampaignConsumer {

    private static final String SMS_BROADCAST_IDENTIFIER = "Broadcast";

    @Autowired
    public SimpleProducer kafkaProducer;

    @Autowired
    private BotService botService;

    // @KafkaListener(id = "${campaign}", topics = "${campaign}")
    public void consumeMessage(String campaignID) throws Exception {
        log.info("CampaignID {}", campaignID);
        processMessage(campaignID)
                .doOnError(s -> log.info(s.getMessage()))
                .subscribe(new Consumer<XMessage>() {
            @Override
            public void accept(XMessage xMessage) {
                log.info("Pushing to : " + TransformerRegistry.getName(xMessage.getTransformers().get(0).getId()));
                try {
                    kafkaProducer.send("com.odk.broadcast", xMessage.toXML());
                } catch (JAXBException e) {
                    e.printStackTrace();
                }
            }
        });

    }

    /**
     * Retrieve a campaign's info from its identifier (Campaign ID)
     *
     * @param campaignID - String {Campaign Identifier}
     * @return XMessage
     */
    public Mono<XMessage> processMessage(String campaignID) throws Exception {
        // Get campaign ID and get campaign details {data: transformers [broadcast(SMS), <formID>(Whatsapp)]}
        return botService
                .getBotNodeFromId(campaignID)
                .doOnError(s -> log.info(s.getMessage()))
                .map(new Function<Result, XMessage>() {
                    @Override
                    public XMessage apply(Result campaignDetails) {
                        ObjectMapper mapper = new ObjectMapper();
//                        JsonNode adapter = campaignDetails.findValues("logic").get(0).get(0).get("adapter");

                        Adapter adapterDto = campaignDetails.getLogicIDs().get(0).getAdapter();

                        // Create a new campaign xMessage
                        XMessagePayload payload = XMessagePayload.builder().text("").build();

//                        String userSegmentName = ((ArrayNode) campaignDetails.get("userSegments")).get(0).get("name").asText();
                        String userSegmentName = campaignDetails.getUsers().get(0).getName();
                        SenderReceiverInfo to = SenderReceiverInfo.builder()
                                .userID(userSegmentName)
                                .build();

                        Transformer broadcast = Transformer.builder()
                                .id("1")
                                .build();
                        ArrayList<Transformer> transformers = new ArrayList<>();
                        transformers.add(broadcast);

                        Map<String, String> metadata = new HashMap<>();
                        SenderReceiverInfo from = SenderReceiverInfo.builder()
                                .userID("admin")
                                .meta(metadata)
                                .build();

                        XMessage.MessageType messageType = XMessage.MessageType.BROADCAST_TEXT;

                        return XMessage.builder()
                                .app(campaignDetails.getName())
                                .channelURI(adapterDto.getChannel())
                                .providerURI(adapterDto.getProvider())
                                .payload(payload)
                                .conversationStage(new ConversationStage(0, ConversationStage.State.STARTING))
                                .timestamp(System.currentTimeMillis())
                                .transformers(transformers)
                                .to(to)
                                .messageType(messageType)
                                .from(from)
                                .build();
                    }
                }).doOnError(e -> {
                    log.error("Error in Campaign Consume::" + e.getMessage());
                });

    }
}