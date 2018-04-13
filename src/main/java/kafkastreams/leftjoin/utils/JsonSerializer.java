package kafkastreams.leftjoin.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return data != null ? objectMapper.writeValueAsBytes(data) : null;
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Can't serialize data [" + data + "] for topic [" + topic + "]", ex);
        }
    }

    @Override
    public void close() {
    }
}
