package org.apache.cassandra.utils.btree;

import com.google.common.base.Function;

/**
 * An interface defining a function to be applied to both the object we are replacing in a BTree and
 * the object that is intended to replace it, returning the object to actually replace it.
 *
 * @param <V>
 */
public interface UpdateFunction<V> extends Function<V, V>
{
    /**
     * @param replacing the value in the original tree we have matched
     * @param update the value in the updating collection that matched
     * @return the value to insert into the new tree
     */
    V apply(V replacing, V update);

    /**
     * @return true if we should fail the update
     */
    boolean abortEarly();

    /**
     * @param heapSize extra heap space allocated (over previous tree)
     */
    void allocated(long heapSize);

}
