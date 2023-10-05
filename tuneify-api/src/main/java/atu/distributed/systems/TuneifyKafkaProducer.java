package atu.distributed.systems;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TuneifyKafkaProducer {

    private Producer<Long, PlayEvent> kafkaProducer;

    public TuneifyKafkaProducer(String bootstrapServers) {
        this.kafkaProducer = createKafkaProducer(bootstrapServers);
    }

    public void publishEvent(PlayEvent playEvent) throws ExecutionException, InterruptedException {
        ProducerRecord<Long, PlayEvent> record = new ProducerRecord<>("plays", playEvent.getUserId(), playEvent);
        RecordMetadata metadata = kafkaProducer.send(record).get();
        System.out.println("Published event to topic " + metadata.topic() + " at offset " + metadata.offset());
    }
    private Producer<Long, PlayEvent> createKafkaProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "TuneifyKafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PlayEvent.PlayEventSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
