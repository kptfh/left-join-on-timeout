package kafkastreams.leftjoin.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonGenericDeserializer<T> implements Deserializer<T>{

    private final ObjectReader objectReader;

    public JsonGenericDeserializer(TypeReference typeReference) {
        this.objectReader = new ObjectMapper().readerFor(typeReference);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return objectReader.readValue(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }
}
