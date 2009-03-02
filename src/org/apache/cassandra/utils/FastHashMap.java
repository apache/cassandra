package org.apache.cassandra.utils;

import java.io.*;
import java.util.*;

/**
 * An implementation of the Map interface which uses an open addressed hash
 * table to store its contents
 * @author Avinash Lakshman
 */
public class FastHashMap<K, V> extends FastObjectHash<K> implements Map<K, V>, Serializable
{
    static final long serialVersionUID = 1L;

    /** the values of the map */
    protected transient V[] values_;

    /**
     * Creates a new <code>FastHashMap</code> instance with the default capacity
     * and load factor.
     */
    public FastHashMap()
    {
        super();
    }

    /**
     * Creates a new <code>FastHashMap</code> instance with a prime capacity
     * equal to or greater than <tt>initialCapacity</tt> and with the default
     * load factor.
     * 
     * @param initialCapacity
     *            an <code>int</code> value
     */
    public FastHashMap(int initialCapacity)
    {
        super(initialCapacity);
    }

    /**
     * Creates a new <code>FastHashMap</code> instance with a prime capacity
     * equal to or greater than <tt>initialCapacity</tt> and with the
     * specified load factor.
     * 
     * @param initialCapacity
     *            an <code>int</code> value
     * @param loadFactor
     *            a <code>float</code> value
     */
    public FastHashMap(int initialCapacity, float loadFactor)
    {
        super(initialCapacity, loadFactor);
    }

    /**
     * Creates a new <code>FastHashMap</code> instance which contains the
     * key/value pairs in <tt>map</tt>.
     * 
     * @param map
     *            a <code>Map</code> value
     */
    public FastHashMap(Map<K, V> map)
    {
        this(map.size());
        putAll(map);
    }

    /**
     * @return a shallow clone of this collection
     */
    public FastHashMap<K, V> clone()
    {
        FastHashMap<K, V> m = (FastHashMap<K, V>) super.clone();
        m.values_ = this.values_.clone();
        return m;
    }

    /**
     * initialize the value array of the map.
     * 
     * @param initialCapacity
     *            an <code>int</code> value
     * @return an <code>int</code> value
     */
    protected int setUp(int initialCapacity)
    {
        int capacity;

        capacity = super.setUp(initialCapacity);
        values_ = (V[]) new Object[capacity];
        return capacity;
    }
    
    void addEntry(Object key, int index)
    {    	
    }
    
    void removeEntry(Object key, int index)
    {
    }

    /**
     * Inserts a key/value pair into the map.
     * 
     * @param key
     *            an <code>Object</code> value
     * @param value
     *            an <code>Object</code> value
     * @return the previous value associated with <tt>key</tt>, or null if
     *         none was found.
     */
    public V put(K key, V value)
    {
        V previous = null;
        Object oldKey;
        int index = insertionIndex(key);
        boolean isNewMapping = true;
        if (index < 0)
        {
            index = -index - 1;
            previous = values_[index];
            isNewMapping = false;
        }
        oldKey = set_[index];
        
        if ( oldKey == FREE )
        {
        	/* This is used as a hook to process new put() operations */
        	addEntry(key, index);
        }
    
        set_[index] = key;
        values_[index] = value;
        if (isNewMapping)
        {
            postInsertHook(oldKey == FREE);
        }

        return previous;
    }

    /**
     * rehashes the map to the new capacity.
     * 
     * @param newCapacity
     *            an <code>int</code> value
     */
    protected void rehash(int newCapacity)
    {
        int oldCapacity = set_.length;
        Object oldKeys[] = set_;
        V oldVals[] = values_;

        set_ = new Object[newCapacity];
        Arrays.fill(set_, FREE);
        values_ = (V[]) new Object[newCapacity];

        for (int i = oldCapacity; i-- > 0;)
        {
            if (oldKeys[i] != FREE && oldKeys[i] != REMOVED)
            {
                Object o = oldKeys[i];
                int index = insertionIndex((K) o);
                if (index < 0)
                {
                    throwObjectContractViolation(set_[(-index - 1)], o);
                }
                set_[index] = o;
                values_[index] = oldVals[i];
            }
        }
    }

    /**
     * retrieves the value for <tt>key</tt>
     * 
     * @param key
     *            an <code>Object</code> value
     * @return the value of <tt>key</tt> or null if no such mapping exists.
     */
    public V get(Object key)
    {
        int index = index((K) key);
        return index < 0 ? null : values_[index];
    }

    /**
     * Empties the map.
     * 
     */
    public void clear()
    {
        if (size() == 0)
            return; // optimization

        super.clear();
        Object[] keys = set_;
        V[] vals = values_;

        for (int i = keys.length; i-- > 0;)
        {
            keys[i] = FREE;
            vals[i] = null;
        }
    }

    /**
     * Deletes a key/value pair from the map.
     * 
     * @param key an <code>Object</code> value
     * @return an <code>Object</code> value
     */
    public V remove(Object key)
    {
        V prev = null;
        int index = index((K)key);
        if (index >= 0)
        {
            prev = values_[index];
            /* clear key,state; adjust size */
            removeAt(index);
            /* This is used as hook to process deleted items */
            removeEntry(key, index);
        }
        return prev;
    }

    /**
     * removes the mapping at <tt>index</tt> from the map.
     * 
     * @param index an <code>int</code> value
     */
    protected void removeAt(int index)
    {
        values_[index] = null;
        /* clear key, state; adjust size */
        super.removeAt(index); 
    }

    /**
     * Returns a view on the values of the map.
     * 
     * @return a <code>Collection</code> value
     */
    public Collection<V> values()
    {
        return Arrays.asList(values_);
    }

    /**
     * returns a Set view on the keys of the map.
     * 
     * @return a <code>Set</code> value
     */
    public Set<K> keySet()
    {
        return new KeyView();
    }

    /**
     * Returns a Set view on the entries of the map.
     * 
     * @return a <code>Set</code> value
     */
    public Set<Map.Entry<K, V>> entrySet()
    {
        throw new UnsupportedOperationException(
                "This operation is currently not supported.");
    }

    /**
     * checks for the presence of <tt>val</tt> in the values of the map.
     * 
     * @param val
     *            an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsValue(Object val)
    {
        Object[] set = set_;
        V[] vals = values_;

        // special case null values so that we don't have to
        // perform null checks before every call to equals()
        if (null == val)
        {
            for (int i = vals.length; i-- > 0;)
            {
                if ((set[i] != FREE && set[i] != REMOVED) && val == vals[i])
                {
                    return true;
                }
            }
        }
        else
        {
            for (int i = vals.length; i-- > 0;)
            {
                if ((set[i] != FREE && set[i] != REMOVED)
                        && (val == vals[i] || val.equals(vals[i])))
                {
                    return true;
                }
            }
        } // end of else
        return false;
    }

    /**
     * checks for the present of <tt>key</tt> in the keys of the map.
     * 
     * @param key
     *            an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean containsKey(Object key)
    {
        return contains(key);
    }

    /**
     * copies the key/value mappings in <tt>map</tt> into this map.
     * 
     * @param map
     *            a <code>Map</code> value
     */
    public void putAll(Map<? extends K, ? extends V> map)
    {
        ensureCapacity(map.size());
        // could optimize this for cases when map instanceof FastHashMap
        for (Iterator<? extends Map.Entry<? extends K, ? extends V>> i = map
                .entrySet().iterator(); i.hasNext();)
        {
            Map.Entry<? extends K, ? extends V> e = i.next();
            put(e.getKey(), e.getValue());
        }
    }

    private abstract class MapBackedView<E> extends AbstractSet<E> implements Set<E>, Iterable<E>
    {
        public abstract Iterator<E> iterator();
        public abstract boolean removeElement(E key);
        public abstract boolean containsElement(E key);

        public boolean contains(Object key)
        {
            return containsElement((E) key);
        }

        public boolean remove(Object o)
        {
            return removeElement((E) o);
        }

        public boolean containsAll(Collection<?> collection)
        {
            for (Iterator i = collection.iterator(); i.hasNext();)
            {
                if (!contains(i.next()))
                {
                    return false;
                }
            }
            return true;
        }

        public void clear()
        {
            FastHashMap.this.clear();
        }

        public boolean add(E obj)
        {
            throw new UnsupportedOperationException();
        }

        public int size()
        {
            return FastHashMap.this.size();
        }

        public Object[] toArray()
        {
            Object[] result = new Object[size()];
            Iterator e = iterator();
            for (int i = 0; e.hasNext(); i++)
                result[i] = e.next();
            return result;
        }

        public <T> T[] toArray(T[] a)
        {
            int size = size();
            if (a.length < size)
                a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

            Iterator<E> it = iterator();
            Object[] result = a;
            for (int i = 0; i < size; i++)
            {
                result[i] = it.next();
            }

            if (a.length > size)
            {
                a[size] = null;
            }

            return a;
        }

        public boolean isEmpty()
        {
            return FastHashMap.this.isEmpty();
        }

        public boolean addAll(Collection<? extends E> collection)
        {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection<?> collection)
        {
            boolean changed = false;
            Iterator i = iterator();
            while (i.hasNext())
            {
                if (!collection.contains(i.next()))
                {
                    i.remove();
                    changed = true;
                }
            }
            return changed;
        }
    }
    
    protected class FastHashMapIterator<T> implements Iterator<T>
    {
        private int nextIndex_;
        private int expectedSize_;
        private FastObjectHash<T> tMap_;
        
        FastHashMapIterator(FastObjectHash<T> tMap)
        {
            nextIndex_ = -1;
            expectedSize_ = tMap.size();
            tMap_ = tMap;
        }
        
        public boolean hasNext()
        {
            return (expectedSize_ > 0);
        }
        
        public T next()
        {
            moveToNextIndex();
            int index = nextIndex_;
            /* 
             * Decrement so that we can track how many 
             * elements we have already looked at.
            */
            --expectedSize_;
            return (T)tMap_.set_[index];
        }
        
        private void moveToNextIndex()
        {
            int i = nextIndex_ + 1;
            for ( ; i < tMap_.set_.length; ++i )
            {
                if ( tMap_.set_[i].equals(FREE) || tMap_.set_[i].equals(REMOVED) )
                {
                    continue;
                }
                else
                {                    
                    break;
                }
            }
            nextIndex_ = i;
        }
        
        public void remove()
        {
            tMap_.removeAt(nextIndex_);
            --expectedSize_;
        }
    }

    /**
     * a view onto the keys of the map.
     */
    protected class KeyView extends MapBackedView<K>
    {
        public Iterator<K> iterator()
        {
            return new FastHashMapIterator(FastHashMap.this);
        }
        
        public boolean removeElement(K key)
        {
            return null != FastHashMap.this.remove(key);
        }

        public boolean containsElement(K key)
        {
            return FastHashMap.this.contains(key);
        }
    }

    final class Entry implements Map.Entry<K, V>
    {
        private K key;
        private V val;
        private final int index;

        Entry(final K key, V value, final int index)
        {
            this.key = key;
            this.val = value;
            this.index = index;
        }

        void setKey(K aKey)
        {
            this.key = aKey;
        }

        void setValue0(V aValue)
        {
            this.val = aValue;
        }

        public K getKey()
        {
            return key;
        }

        public V getValue()
        {
            return val;
        }

        public V setValue(V o)
        {
            if (values_[index] != val)
            {
                throw new ConcurrentModificationException();
            }
            values_[index] = o;
            o = val; // need to return previous value
            val = o; // update this entry's value, in case
            // setValue is called again
            return o;
        }

        public boolean equals(Object o)
        {
            if (o instanceof Map.Entry)
            {
                Map.Entry e1 = this;
                Map.Entry e2 = (Map.Entry) o;
                return (e1.getKey() == null ? e2.getKey() == null : e1.getKey().equals(e2.getKey()))
                        && (e1.getValue() == null ? e2.getValue() == null : e1.getValue().equals(e2.getValue()));
            }
            return false;
        }

        public int hashCode()
        {
            return (getKey() == null ? 0 : getKey().hashCode()) ^ (getValue() == null ? 0 : getValue().hashCode());
        }
    }
    
    public static void main(String[] args) throws Throwable
    {
        Map<String, String> map = new FastHashMap<String, String>();
        map.put("Avinash", "Avinash");
        map.put("Avinash", "Srinivas");
    }
}
