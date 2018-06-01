package kafkastreams.leftjoin.utils;

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static kafkastreams.leftjoin.utils.MultiMapUtils.addToMultiMap;
import static kafkastreams.leftjoin.utils.MultiMapUtils.removeFromMultiMap;
import static org.assertj.core.api.Assertions.assertThat;

public class MultiMapUtilsTest {

    public static final String KEY1 = "key1";
    public static final String VALUE1 = "value1";
    public static final String VALUE2 = "value2";

    private Map<String, List<String>> multiMap = new HashMap<>();

    @Test
    public void shouldAddIfNotPresent(){
        addToMultiMap(multiMap, KEY1, k -> VALUE1);

        assertThat(multiMap.get(KEY1)).containsExactly(VALUE1);
    }

    @Test
    public void shouldAddIfPresent(){
        addToMultiMap(multiMap, KEY1, k -> VALUE1);
        addToMultiMap(multiMap, KEY1, k -> VALUE2);

        assertThat(multiMap.get(KEY1)).containsExactly(VALUE1, VALUE2);
    }

    @Test
    public void shouldRemoveExactValue(){
        addToMultiMap(multiMap, KEY1, k -> VALUE1);
        addToMultiMap(multiMap, KEY1, k -> VALUE2);
        removeFromMultiMap(multiMap, KEY1, VALUE1);
        assertThat(multiMap.get(KEY1)).containsExactly(VALUE2);
    }

    @Test
    public void shouldRemoveEntryIfEmpty(){
        addToMultiMap(multiMap, KEY1, k -> VALUE1);
        removeFromMultiMap(multiMap, KEY1, VALUE1);
        assertThat(multiMap.get(KEY1)).isNull();
    }

}
