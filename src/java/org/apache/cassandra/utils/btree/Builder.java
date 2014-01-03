package org.apache.cassandra.utils.btree;

import java.util.Collection;
import java.util.Comparator;

import com.google.common.base.Function;

import static org.apache.cassandra.utils.btree.BTree.EMPTY_BRANCH;
import static org.apache.cassandra.utils.btree.BTree.EMPTY_LEAF;
import static org.apache.cassandra.utils.btree.BTree.FAN_SHIFT;
import static org.apache.cassandra.utils.btree.BTree.POSITIVE_INFINITY;

/**
 * A class for constructing a new BTree, either from an existing one and some set of modifications
 * or a new tree from a sorted collection of items.
 * <p/>
 * This is a fairly heavy-weight object, so a ThreadLocal instance is created for making modifications to a tree
 */
final class Builder
{
    private final NodeBuilder rootBuilder = new NodeBuilder();

    /**
     * At the highest level, we adhere to the classic b-tree insertion algorithm:
     *
     * 1. Add to the appropriate leaf
     * 2. Split the leaf if necessary, add the median to the parent
     * 3. Split the parent if necessary, etc.
     *
     * There is one important difference: we don't actually modify the original tree, but copy each node that we
     * modify.  Note that every node on the path to the key being inserted or updated will be modified; this
     * implies that at a minimum, the root node will be modified for every update, so every root is a "snapshot"
     * of a tree that can be iterated or sliced without fear of concurrent modifications.
     *
     * The NodeBuilder class handles the details of buffering the copied contents of the original tree and
     * adding in our changes.  Since NodeBuilder maintains parent/child references, it also handles parent-splitting
     * (easy enough, since any node affected by the split will already be copied into a NodeBuilder).
     *
     * One other difference from the simple algorithm is that we perform modifications in bulk;
     * we assume @param source has been sorted, e.g. by BTree.update, so the update of each key resumes where
     * the previous left off.
     */
    public <V> Object[] update(Object[] btree, Comparator<V> comparator, Collection<V> source, ReplaceFunction<V> replaceF, Function<?, Boolean> terminateEarly)
    {
        NodeBuilder current = rootBuilder;
        current.reset(btree, POSITIVE_INFINITY);

        for (V key : source)
        {
            while (true)
            {
                if (terminateEarly != null && terminateEarly.apply(null) == Boolean.TRUE)
                {
                    rootBuilder.clear();
                    return null;
                }
                NodeBuilder next = current.update(key, comparator, replaceF);
                if (next == null)
                    break;
                // we were in a subtree from a previous key that didn't contain this new key;
                // retry against the correct subtree
                current = next;
            }
        }

        // finish copying any remaining keys from the original btree
        while (true)
        {
            NodeBuilder next = current.update(POSITIVE_INFINITY, comparator, replaceF);
            if (next == null)
                break;
            current = next;
        }

        // updating with POSITIVE_INFINITY means that current should be back to the root
        assert current.isRoot();

        Object[] r = current.toNode();
        current.clear();
        return r;
    }

    public <V> Object[] build(Collection<V> source, int size)
    {
        NodeBuilder current = rootBuilder;
        // we descend only to avoid wasting memory; in update() we will often descend into existing trees
        // so here we want to descend also, so we don't have lg max(N) depth in both directions
        while ((size >>= FAN_SHIFT) > 0)
            current = current.ensureChild();

        current.reset(EMPTY_LEAF, POSITIVE_INFINITY);
        for (V key : source)
            current.addNewKey(key, null);

        current = current.ascendToRoot();

        Object[] r = current.toNode();
        current.clear();
        return r;
    }
}