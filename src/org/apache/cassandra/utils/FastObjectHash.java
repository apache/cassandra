package org.apache.cassandra.utils;

import java.util.Arrays;

/**
 * An open addressed hashing implementation for Object types.
 * 
 * @author Avinash Lakshman
 */
abstract public class FastObjectHash<T> extends FastHash
{
    static final long serialVersionUID = -3461112548087185871L;

    /** the set of Objects */
    protected transient Object[] set_;
    protected static final Object REMOVED = new Object(), FREE = new Object();

    /**
     * Creates a new <code>TObjectHash</code> instance with the default
     * capacity and load factor.
     */
    public FastObjectHash()
    {
        super();        
    }

    /**
     * Creates a new <code>TObjectHash</code> instance whose capacity is the
     * next highest prime above <tt>initialCapacity + 1</tt> unless that value
     * is already prime.
     * 
     * @param initialCapacity
     *            an <code>int</code> value
     */
    public FastObjectHash(int initialCapacity)
    {
        super(initialCapacity);        
    }

    /**
     * Creates a new <code>TObjectHash</code> instance with a prime value at
     * or near the specified capacity and load factor.
     * 
     * @param initialCapacity
     *            used to find a prime capacity for the table.
     * @param loadFactor
     *            used to calculate the threshold over which rehashing takes
     *            place.
     */
    public FastObjectHash(int initialCapacity, float loadFactor)
    {
        super(initialCapacity, loadFactor);        
    }

    /**
     * @return a shallow clone of this collection
     */
    public FastObjectHash<T> clone()
    {
        FastObjectHash<T> h = (FastObjectHash<T>) super.clone();
        h.set_ = (Object[]) this.set_.clone();
        return h;
    }
    
    /**
     * This method is invoked every time a key is
     * added into the Map.
     * @param key key that is inserted
     * @param index index position of the key being 
     *              inserted
     */
    abstract void addEntry(Object key, int index);
    
    /**
     * This method is invoked every time a key is
     * deleted from the Map.
     * @param key key being deleted
     * @param index index position of the key being
     *              deleted
     */
    abstract void removeEntry(Object key, int index);

    protected int capacity()
    {
        return set_.length;
    }

    protected void removeAt(int index)
    {
        set_[index] = REMOVED;
        super.removeAt(index);
    }

    /**
     * initializes the Object set of this hash table.
     * 
     * @param initialCapacity
     *            an <code>int</code> value
     * @return an <code>int</code> value
     */
    protected int setUp(int initialCapacity)
    {
        int capacity;

        capacity = super.setUp(initialCapacity);
        set_ = new Object[capacity];
        Arrays.fill(set_, FREE);
        return capacity;
    }

    /**
     * Searches the set for <tt>obj</tt>
     * 
     * @param obj
     *            an <code>Object</code> value
     * @return a <code>boolean</code> value
     */
    public boolean contains(Object obj)
    {
        return index((T) obj) >= 0;
    }

    /**
     * Locates the index of <tt>obj</tt>.
     * 
     * @param obj
     *            an <code>Object</code> value
     * @return the index of <tt>obj</tt> or -1 if it isn't in the set.
     */
    protected int index(Object obj)
    {        
        final Object[] set = set_;
        final int length = set.length;
        final int hash = obj.hashCode() & 0x7fffffff;
        int index = hash % length;
        Object cur = set[index];

        if (cur == FREE)
            return -1;

        // NOTE: here it has to be REMOVED or FULL (some user-given value)
        if (cur == REMOVED || cur.equals(obj))
        {
            // see Knuth, p. 529
            final int probe = 1 + (hash % (length - 2));

            while (cur != FREE&& (cur == REMOVED || !cur.equals(obj)))
            {
                index -= probe;
                if (index < 0)
                {
                    index += length;
                }
                cur = set[index];
            }            
        }

        return cur == FREE ? -1 : index;
    }

    /**
     * Locates the index at which <tt>obj</tt> can be inserted. if there is
     * already a value equal()ing <tt>obj</tt> in the set, returns that
     * value's index as <tt>-index - 1</tt>.
     * 
     * @param obj
     *            an <code>Object</code> value
     * @return the index of a FREE slot at which obj can be inserted or, if obj
     *         is already stored in the hash, the negative value of that index,
     *         minus 1: -index -1.
     */
    protected int insertionIndex(T obj)
    {        
        final Object[] set = set_;
        final int length = set.length;
        final int hash = obj.hashCode() & 0x7fffffff;
        int index = hash % length;
        Object cur = set[index];

        if (cur == FREE)
        {
            return index; // empty, all done
        }
        else if (cur != REMOVED && cur.equals(obj))
        {
            return -index - 1; // already stored
        }
        else
        { // already FULL or REMOVED, must probe
            // compute the double token
            final int probe = 1 + (hash % (length - 2));

            // if the slot we landed on is FULL (but not removed), probe
            // until we find an empty slot, a REMOVED slot, or an element
            // equal to the one we are trying to insert.
            // finding an empty slot means that the value is not present
            // and that we should use that slot as the insertion point;
            // finding a REMOVED slot means that we need to keep searching,
            // however we want to remember the offset of that REMOVED slot
            // so we can reuse it in case a "new" insertion (i.e. not an update)
            // is possible.
            // finding a matching value means that we've found that our desired
            // key is already in the table
            if (cur != REMOVED)
            {
                // starting at the natural offset, probe until we find an
                // offset that isn't full.
                do
                {
                    index -= probe;
                    if (index < 0)
                    {
                        index += length;
                    }
                    cur = set[index];
                }
                while (cur != FREE && cur != REMOVED
                        && !cur.equals(obj));
            }

            // if the index we found was removed: continue probing until we
            // locate a free location or an element which equal()s the
            // one we have.
            if (cur == REMOVED)
            {
                int firstRemoved = index;
                while (cur != FREE
                        && (cur == REMOVED || !cur.equals(obj)))
                {
                    index -= probe;
                    if (index < 0)
                    {
                        index += length;
                    }
                    cur = set[index];
                }
                // NOTE: cur cannot == REMOVED in this block
                return (cur != FREE) ? -index - 1 : firstRemoved;
            }
            // if it's full, the key is already stored
            // NOTE: cur cannot equal REMOVE here (would have retuned already
            // (see above)
            return (cur != FREE) ? -index - 1 : index;
        }
    }

    /**
     * This is the default implementation of TObjectHashingStrategy: it
     * delegates hashing to the Object's hashCode method.
     * 
     * @param o
     *            for which the hashcode is to be computed
     * @return the hashCode
     * @see Object#hashCode()
     */
    public final int computeHashCode(T o)
    {
        return o == null ? 0 : o.hashCode();
    }

    /**
     * This is the default implementation of TObjectHashingStrategy: it
     * delegates equality comparisons to the first parameter's equals() method.
     * 
     * @param o1
     *            an <code>Object</code> value
     * @param o2
     *            an <code>Object</code> value
     * @return true if the objects are equal
     * @see Object#equals(Object)
     */
    public final boolean equals(T o1, T o2)
    {
        return o1 == null ? o2 == null : o1.equals(o2);
    }

    /**
     * Convenience methods for subclasses to use in throwing exceptions about
     * badly behaved user objects employed as keys. We have to throw an
     * IllegalArgumentException with a rather verbose message telling the user
     * that they need to fix their object implementation to conform to the
     * general contract for java.lang.Object.
     * 
     * @param o1
     *            the first of the equal elements with unequal hash codes.
     * @param o2
     *            the second of the equal elements with unequal hash codes.
     * @exception IllegalArgumentException
     *                the whole point of this method.
     */
    protected final void throwObjectContractViolation(Object o1, Object o2)
            throws IllegalArgumentException
    {
        throw new IllegalArgumentException(
                "Equal objects must have equal hashcodes. "
                        + "During rehashing, Trove discovered that "
                        + "the following two objects claim to be "
                        + "equal (as in java.lang.Object.equals()) "
                        + "but their hashCodes (or those calculated by "
                        + "your TObjectHashingStrategy) are not equal."
                        + "This violates the general contract of "
                        + "java.lang.Object.hashCode().  See bullet point two "
                        + "in that method's documentation. " + "object #1 ="
                        + o1 + "; object #2 =" + o2);
    }
} // TObjectHash
