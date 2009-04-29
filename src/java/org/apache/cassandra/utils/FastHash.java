package org.apache.cassandra.utils;

import java.util.Random;


/**
 * Base class for hashtables that use open addressing to resolve collisions.
 * 
 * @author Avinash Lakshman
 * 
 */

abstract public class FastHash implements Cloneable
{
    /** the current number of occupied slots in the hash. */
    protected transient int size_;
    
    /** the current number of free slots in the hash. */
    protected transient int free_;
    
    /** the load above which rehashing occurs. */
    protected static final float DEFAULT_LOAD_FACTOR = 0.5f;
    
    /**
     * the default initial capacity for the hash table. This is one less than a
     * prime value because one is added to it when searching for a prime
     * capacity to account for the free slot required by open addressing. Thus,
     * the real default capacity is 11.
     */
    protected static final int DEFAULT_INITIAL_CAPACITY = 10;
    
    /**
     * Determines how full the internal table can become before rehashing is
     * required. This must be a value in the range: 0.0 < loadFactor < 1.0. The
     * default value is 0.5, which is about as large as you can get in open
     * addressing without hurting performance. Cf. Knuth, Volume 3., Chapter 6.
     */
    protected float loadFactor_;
    
    /**
     * The maximum number of elements allowed without allocating more space.
     */
    protected int maxSize_;
    
    /**
     * The number of removes that should be performed before an auto-compaction
     * occurs.
     */
    protected int autoCompactRemovesRemaining_;
    
    /**
     * The auto-compaction factor for the table.
     * 
     * @see #setAutoCompactionFactor
     */
    protected float autoCompactionFactor_;
    
    /**
     * @see
     */
    private boolean autoCompactTemporaryDisable_ = false;
    
    /**
     * Creates a new <code>THash</code> instance with the default capacity and
     * load factor.
     */
    public FastHash()
    {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
    }
    
    /**
     * Creates a new <code>THash</code> instance with a prime capacity at or
     * near the specified capacity and with the default load factor.
     * 
     * @param initialCapacity
     *            an <code>int</code> value
     */
    public FastHash(int initialCapacity)
    {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }
    
    /**
     * Creates a new <code>THash</code> instance with a prime capacity at or
     * near the minimum needed to hold <tt>initialCapacity</tt> elements with
     * load factor <tt>loadFactor</tt> without triggering a rehash.
     * 
     * @param initialCapacity
     *            an <code>int</code> value
     * @param loadFactor
     *            a <code>float</code> value
     */
    public FastHash(int initialCapacity, float loadFactor)
    {
        super();
        loadFactor_ = loadFactor;
        
        // Through testing, the load factor (especially the default load factor)
        // has been
        // found to be a pretty good starting auto-compaction factor.
        autoCompactionFactor_ = loadFactor;
        
        setUp((int) Math.ceil(initialCapacity / loadFactor));
    }
    
    public Object clone()
    {
        try
        {
            return super.clone();
        }
        catch (CloneNotSupportedException cnse)
        {
            return null; // it's supported
        }
    }
    
    /**
     * Tells whether this set is currently holding any elements.
     * 
     * @return a <code>boolean</code> value
     */
    public boolean isEmpty()
    {
        return 0 == size_;
    }
    
    /**
     * Returns the number of distinct elements in this collection.
     * 
     * @return an <code>int</code> value
     */
    public int size()
    {
        return size_;
    }
    
    /**
     * @return the current physical capacity of the hash table.
     */
    abstract protected int capacity();
    
    /**
     * Ensure that this hashtable has sufficient capacity to hold
     * <tt>desiredCapacity<tt> <b>additional</b> elements without
     * requiring a rehash.  This is a tuning method you can call
     * before doing a large insert.
     *
     * @param desiredCapacity an <code>int</code> value
     */
    public void ensureCapacity(int desiredCapacity)
    {
        if (desiredCapacity > (maxSize_ - size()))
        {
            rehash(PrimeFinder.nextPrime((int) Math.ceil(desiredCapacity
                    + size() / loadFactor_) + 1));
            computeMaxSize(capacity());
        }
    }
    
    /**
     * Compresses the hashtable to the minimum prime size (as defined by
     * PrimeFinder) that will hold all of the elements currently in the table.
     * If you have done a lot of <tt>remove</tt> operations and plan to do a
     * lot of queries or insertions or iteration, it is a good idea to invoke
     * this method. Doing so will accomplish two things:
     * 
     * <ol>
     * <li> You'll free memory allocated to the table but no longer needed
     * because of the remove()s.</li>
     * 
     * <li> You'll get better query/insert/iterator performance because there
     * won't be any <tt>REMOVED</tt> slots to skip over when probing for
     * indices in the table.</li>
     * </ol>
     */
    public void compact()
    {
        // need at least one free spot for open addressing
        rehash(PrimeFinder.nextPrime((int) Math.ceil(size() / loadFactor_) + 1));
        computeMaxSize(capacity());
        
        // If auto-compaction is enabled, re-determine the compaction interval
        if (autoCompactionFactor_ != 0)
        {
            computeNextAutoCompactionAmount(size());
        }
    }
    
    /**
     * The auto-compaction factor controls whether and when a table performs a
     * {@link #compact} automatically after a certain number of remove
     * operations. If the value is non-zero, the number of removes that need to
     * occur for auto-compaction is the size of table at the time of the
     * previous compaction (or the initial capacity) multiplied by this factor.
     * <p>
     * Setting this value to zero will disable auto-compaction.
     */
    public void setAutoCompactionFactor(float factor)
    {
        if (factor < 0)
        {
            throw new IllegalArgumentException("Factor must be >= 0: " + factor);
        }
        
        autoCompactionFactor_ = factor;
    }
    
    /**
     * @see #setAutoCompactionFactor
     */
    public float getAutoCompactionFactor()
    {
        return autoCompactionFactor_;
    }
    
    /**
     * This simply calls {@link #compact compact}. It is included for symmetry
     * with other collection classes. Note that the name of this method is
     * somewhat misleading (which is why we prefer <tt>compact</tt>) as the
     * load factor may require capacity above and beyond the size of this
     * collection.
     * 
     * @see #compact
     */
    public final void trimToSize()
    {
        compact();
    }
    
    /**
     * Delete the record at <tt>index</tt>. Reduces the size of the
     * collection by one.
     * 
     * @param index
     *            an <code>int</code> value
     */
    protected void removeAt(int index)
    {
        size_--;
        
        // If auto-compaction is enabled, see if we need to compact
        if (autoCompactionFactor_ != 0)
        {
            autoCompactRemovesRemaining_--;
            
            if (!autoCompactTemporaryDisable_
                    && autoCompactRemovesRemaining_ <= 0)
            {
                // Do the compact
                // NOTE: this will cause the next compaction interval to be
                // calculated
                compact();
            }
        }
    }
    
    /**
     * Empties the collection.
     */
    public void clear()
    {
        size_ = 0;
        free_ = capacity();
    }
    
    /**
     * initializes the hashtable to a prime capacity which is at least
     * <tt>initialCapacity + 1</tt>.
     * 
     * @param initialCapacity
     *            an <code>int</code> value
     * @return the actual capacity chosen
     */
    protected int setUp(int initialCapacity)
    {
        int capacity;
        
        capacity = PrimeFinder.nextPrime(initialCapacity);
        computeMaxSize(capacity);
        computeNextAutoCompactionAmount(initialCapacity);
        
        return capacity;
    }
    
    /**
     * Rehashes the set.
     * 
     * @param newCapacity
     *            an <code>int</code> value
     */
    protected abstract void rehash(int newCapacity);
    
    /**
     * Temporarily disables auto-compaction. MUST be followed by calling
     * {@link #reenableAutoCompaction}.
     */
    protected void tempDisableAutoCompaction()
    {
        autoCompactTemporaryDisable_ = true;
    }
    
    /**
     * Re-enable auto-compaction after it was disabled via
     * {@link #tempDisableAutoCompaction()}.
     * 
     * @param check_for_compaction
     *            True if compaction should be performed if needed before
     *            returning. If false, no compaction will be performed.
     */
    protected void reenableAutoCompaction(boolean check_for_compaction)
    {
        autoCompactTemporaryDisable_ = false;
        
        if (check_for_compaction && autoCompactRemovesRemaining_ <= 0
                && autoCompactionFactor_ != 0)
        {
            
            // Do the compact
            // NOTE: this will cause the next compaction interval to be
            // calculated
            compact();
        }
    }
    
    /**
     * Computes the values of maxSize. There will always be at least one free
     * slot required.
     * 
     * @param capacity
     *            an <code>int</code> value
     */
    private final void computeMaxSize(int capacity)
    {
        // need at least one free slot for open addressing
        maxSize_ = Math.min(capacity - 1, (int) Math.floor(capacity
                * loadFactor_));
        free_ = capacity - size_; // reset the free element count
    }
    
    /**
     * Computes the number of removes that need to happen before the next
     * auto-compaction will occur.
     */
    private void computeNextAutoCompactionAmount(int size)
    {
        if (autoCompactionFactor_ != 0)
        {
            autoCompactRemovesRemaining_ = Math.round(size
                    * autoCompactionFactor_);
        }
    }
    
    /**
     * After an insert, this hook is called to adjust the size/free values of
     * the set and to perform rehashing if necessary.
     */
    protected final void postInsertHook(boolean usedFreeSlot)
    {
        if (usedFreeSlot)
        {
            free_--;
        }
        
        // rehash whenever we exhaust the available space in the table
        if (++size_ > maxSize_ || free_ == 0)
        {
            // choose a new capacity suited to the new state of the table
            // if we've grown beyond our maximum size, double capacity;
            // if we've exhausted the free spots, rehash to the same capacity,
            // which will free up any stale removed slots for reuse.
            int newCapacity = size_ > maxSize_ ? PrimeFinder
                    .nextPrime(capacity() << 1) : capacity();
                    rehash(newCapacity);
                    computeMaxSize(capacity());
        }
    }
    
    protected int calculateGrownCapacity()
    {
        return capacity() << 1;
    }
}// THash
