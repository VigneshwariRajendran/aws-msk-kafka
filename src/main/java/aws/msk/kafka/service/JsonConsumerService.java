package aws.msk.kafka.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class JsonConsumerService {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @KafkaListener(topics = "your-topic-name", groupId = "my-testgroup-id")
        public void consume(String message) {
            System.out.println("consume method running........");
            try {
                JsonNode rootNode = objectMapper.readTree(message);

                // Extracting only the needed fields
                String time = rootNode.path("time").asText();
                String name = rootNode.path("fiscorrelationid").asText();


                System.out.println("Consumed message - time: " + time + ", Name: " + name);

                // Further processing using only the extracted data

            } catch (IOException e) {
                e.printStackTrace();

            }
        }


}
