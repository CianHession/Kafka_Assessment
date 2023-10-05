import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public class PlayEvent {
    long userId;
    String artistName;
    String trackName;
    String albumName;

    public PlayEvent() {
    }

    public PlayEvent(long userId, String artistName, String trackName, String albumName) {
        this.userId = userId;
        this.artistName = artistName;
        this.trackName = trackName;
        this.albumName = albumName;
    }

    public String getArtistName() {
        return artistName;
    }

    public void setArtistName(String artistName) {
        this.artistName = artistName;
    }

    public String getTrackName() {
        return trackName;
    }

    public void setTrackName(String trackName) {
        this.trackName = trackName;
    }

    public String getAlbumName() {
        return albumName;
    }

    public void setAlbumName(String albumName) {
        this.albumName = albumName;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public static class PlayEventSerializer implements Serializer<PlayEvent> {
        @Override
        public byte[] serialize(String topic, PlayEvent data) {
            byte[] serializedData = null;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                serializedData = objectMapper.writeValueAsString(data).getBytes();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return serializedData;
        }
    }

    /**
     * Kafka Deserializer implementation.
     * Deserializes a Transaction from JSON to a {@link PlayEvent} object
     */
    public static class PlayEventDeserializer implements Deserializer<PlayEvent> {

        @Override
        public PlayEvent deserialize(String topic, byte[] data) {
            ObjectMapper mapper = new ObjectMapper();
            PlayEvent transaction = null;
            try {
                transaction = mapper.readValue(data, PlayEvent.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return transaction;

        }
    }


}
