package kafkastreams.leftjoin.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class MultiMapUtils {

    private static Logger log = LoggerFactory.getLogger(MultiMapUtils.class);

    public static <K, V> void addToMultiMap(Map<K, List<V>> multiMap, K key, Function<K, V> valueFunction){
        multiMap.compute(key, (key_, values) -> {
            List<V> valuesMerged = values != null ? values : new ArrayList<>(1);
            valuesMerged.add(valueFunction.apply(key_));
            return valuesMerged;
        });
    }

    public static <K, V> void removeFromMultiMap(Map<K, List<V>> multiMap, K key, V value){
        multiMap.compute(key, (key_, values) -> {
            if(values == null){
                log.warn("Values is absent in multimap for key {}", key_);
                return null;
            }
            boolean removed = values.remove(value);
            if(!removed){
                log.warn("Value {} is absent in multimap for key {}", value, key_);
            }
            return values.size() == 0 ? null : values;
        });
    }


}
