import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TuneifyReportingService {
    private static final String PLAYS_TOPIC = "plays";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        String consumerGroup = "defaultConsumerGroup";

        TuneifyReportingService trs = new TuneifyReportingService();
        System.out.println("Consumer is part of consumer group " + consumerGroup);
        Consumer<Long, PlayEvent> kafkaConsumer = createKafkaConsumer(BOOTSTRAP_SERVERS,
                consumerGroup);
        consumeMessages(PLAYS_TOPIC, kafkaConsumer);
    }

    public static void consumeMessages(String topic, Consumer<Long, PlayEvent> kafkaConsumer) {
        kafkaConsumer.subscribe(Collections.singletonList(topic));

        while (true) {
            ConsumerRecords<Long, PlayEvent> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            if (consumerRecords.isEmpty()) {
                System.out.println("No records found");
                return;
            } else {
                consumerRecords.forEach(record -> {
                    System.out.println("Received record (key=" + record.key() + ", value=" + record.value() + ") at offset " + record.offset());
                });
            }
            // do something with the records
            kafkaConsumer.commitAsync();
        }

    }

    public static Consumer<Long, PlayEvent> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new KafkaConsumer<>(properties);
    }

    private static void processRecord(PlayEvent playEvent) {
        System.out.println(
                String.format("** REPORTING: RECEIVED KAFKA EVENT -  userId - %s, artist - %s, track - %s, album - %s",
                        playEvent.getUserId(),
                        playEvent.getArtistName(),
                        playEvent.getTrackName(),
                        playEvent.getAlbumName()));
    }
}
