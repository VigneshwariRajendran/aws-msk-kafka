package aws.msk.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;

@Service
public class JsonConsumerService {

        private final ObjectMapper objectMapper = new ObjectMapper();

        private final Map<String, List<JsonNode>> groupedMessages = new HashMap<>();

        @KafkaListener(topics = "your-topic-name", groupId = "my-testgroup-id")
        public void consume(String message) {
            System.out.println("consume method running........");
            try {
                JsonNode rootNode = objectMapper.readTree(message);

                // Extracting only the needed fields
                String time = rootNode.path("time").asText();
                String name = rootNode.path("fiscorrelationid").asText();


                System.out.println("Consumed message - time: " + time + ", Name: " + name);
                String evtNme = rootNode.path("data").path("adtDtl").path("evtNme").asText();

                //group based on evtNme
                groupedMessages.computeIfAbsent(evtNme, k -> new ArrayList<>()).add(rootNode);

                //process the grouped
                processGroupedMessages();

            } catch (IOException e) {
                e.printStackTrace();

            }
        }
    public void processGroupedMessages() {
        groupedMessages.forEach((evtNme, messages) -> {
            messages.sort(Comparator.comparing(msg -> msg.path("time").asText()));

            System.out.println("Processed group: " + evtNme);
            for (JsonNode message : messages) {
                System.out.println(message.toString());
            }
        });
    }


}
