/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.utils.btree;

import java.util.Comparator;

import io.netty.util.Recycler;

import static org.apache.cassandra.utils.btree.BTree.POSITIVE_INFINITY;

/**
 * A class for constructing a new BTree, either from an existing one and some set of modifications
 * or a new tree from a sorted collection of items.
 * <p/>
 * This is a fairly heavy-weight object, so a Recycled instance is created for making modifications to a tree
 */
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9769
final class TreeBuilder
{

    private final static Recycler<TreeBuilder> builderRecycler = new Recycler<TreeBuilder>()
    {
        protected TreeBuilder newObject(Handle handle)
        {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9766
            return new TreeBuilder(handle);
        }
    };

    public static TreeBuilder newInstance()
    {
        return builderRecycler.get();
    }

    private final Recycler.Handle recycleHandle;
    private final NodeBuilder rootBuilder = new NodeBuilder();

    private TreeBuilder(Recycler.Handle handle)
    {
        this.recycleHandle = handle;
    }

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
    public <C, K extends C, V extends C> Object[] update(Object[] btree, Comparator<C> comparator, Iterable<K> source, UpdateFunction<K, V> updateF)
    {
        assert updateF != null;
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6692

        NodeBuilder current = rootBuilder;
        current.reset(btree, POSITIVE_INFINITY, updateF, comparator);

//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-8099
        for (K key : source)
        {
            while (true)
            {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6692
                if (updateF.abortEarly())
                {
                    rootBuilder.clear();
                    return null;
                }
                NodeBuilder next = current.update(key);
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
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-6934
            NodeBuilder next = current.finish();
            if (next == null)
                break;
            current = next;
        }

        // updating with POSITIVE_INFINITY means that current should be back to the root
        assert current.isRoot();

        Object[] r = current.toNode();
        current.clear();

        recycleHandle.recycle(this);
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-9989

        return r;
    }
}
