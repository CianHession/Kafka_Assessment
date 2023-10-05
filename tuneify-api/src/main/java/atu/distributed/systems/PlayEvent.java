package atu.distributed.systems;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.math.BigInteger;

public class PlayEvent {
    long userId;
    String artistName;
    String trackName;
    String albumName;

    public PlayEvent(long userId, String artistName, String trackName, String albumName) {
        this.userId = userId;
        this.artistName = artistName;
        this.trackName = trackName;
        this.albumName = albumName;
    }

    public PlayEvent(BigInteger result) {
        this.userId = result.longValue();
        this.artistName = result.toString();
        this.trackName = result.toString();
        this.albumName = result.toString();
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

    @Override
    public String toString() {
        return "PlayEvent{" +
                "userId=" + userId +
                ", artistName='" + artistName + '\'' +
                ", trackName='" + trackName + '\'' +
                ", albumName='" + albumName + '\'' +
                '}';
    }
}
