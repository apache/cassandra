package org.apache.cassandra.utils;

import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;

public class SortedBiMultiValMap<K, V> extends BiMultiValMap<K, V>
{
    protected SortedBiMultiValMap(SortedMap<K, V> forwardMap, SortedSetMultimap<V, K> reverseMap)
    {
        super(forwardMap, reverseMap);
    }

    public static <K extends Comparable<K>, V extends Comparable<V>> SortedBiMultiValMap<K, V> create()
    {
        return new SortedBiMultiValMap<K, V>(new TreeMap<K,V>(), TreeMultimap.<V, K>create());
    }

    public static <K, V> SortedBiMultiValMap<K, V> create(Comparator<K> keyComparator, Comparator<V> valueComparator)
    {
        if (keyComparator == null)
            keyComparator = defaultComparator();
        if (valueComparator == null)
            valueComparator = defaultComparator();
        return new SortedBiMultiValMap<K, V>(new TreeMap<K,V>(keyComparator), TreeMultimap.<V, K>create(valueComparator, keyComparator));
    }

    public static <K extends Comparable<K>, V extends Comparable<V>> SortedBiMultiValMap<K, V> create(BiMultiValMap<K, V> map)
    {
        SortedBiMultiValMap<K, V> newMap = SortedBiMultiValMap.<K,V>create();
        newMap.forwardMap.putAll(map);
        newMap.reverseMap.putAll(map.inverse());
        return newMap;
    }

    public static <K, V> SortedBiMultiValMap<K, V> create(BiMultiValMap<K, V> map, Comparator<K> keyComparator, Comparator<V> valueComparator)
    {
        SortedBiMultiValMap<K, V> newMap = create(keyComparator, valueComparator);
        newMap.forwardMap.putAll(map);
        newMap.reverseMap.putAll(map.inverse());
        return newMap;
    }

    private static <T> Comparator<T> defaultComparator()
    {
        return new Comparator<T>()
        {
            @SuppressWarnings("unchecked")
            public int compare(T o1, T o2)
            {
                return ((Comparable<T>) o1).compareTo(o2);
            }
        };
    }
}
