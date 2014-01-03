package org.apache.cassandra.utils.btree;

/**
 * An interface defining a function to be applied to both the object we are replacing in a BTree and
 * the object that is intended to replace it, returning the object to actually replace it.
 *
 * @param <V>
 */
public interface ReplaceFunction<V>
{
    V apply(V replaced, V update);
}
