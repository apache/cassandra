/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.tries;

import org.apache.cassandra.utils.AbstractIterator;

/**
 * Utility class for performing some walks over the trie that result in an iterator of items.
 * See TrieValuesIterator and TrieEntriesIterator for usage.
 */
abstract class TrieIterator<T, L, V> extends AbstractIterator<V>
{
    private Trie.Node<T, L> current;

    protected TrieIterator(Trie<T> trie)
    {
        current = trie.root();
        if (current == null)
            endOfData();
    }

    protected V computeNext()
    {
        Trie.Remaining has = startIteration();

        while (true)
        {
            if (has != null)
            {
                // We have a transition, get child to descend into
                Trie.Node<T, L> child = getChild(current, has);

                if (child == null)
                {
                    // no child, get next
                    has = advanceIteration();
                }
                else
                {
                    // Enter node
                    current = child;
                    // Check payload
                    V v = contentOf(child);
                    if (v != null)
                        return v; // payload was produced, wait for next()

                    has = startIteration();
                }
            }
            else
            {
                // There are no more children. Ascend to the parent state to continue walk.
                current = exitNodeAndReturnParent(current);
                if (current == null)
                {
                    // We've reached back the root, our walk is finished
                    return endOfData();
                }
                has = advanceIteration();
            }
        }
    }

    /**
     * Start the iteration on a node. Can be overridden by children (e.g. to skip processing branch).
     */
    Trie.Remaining startIteration()
    {
        return current.startIteration();
    }

    /**
     * Advance the iteration on a node. Can be overridden by children (e.g. to skip processing selected transitions).
     */
    Trie.Remaining advanceIteration()
    {
        return current.advanceIteration();
    }

    // The methods below are to be overridden by subclasses.

    /**
     * Called by advance to descend into a child node.
     */
    abstract Trie.Node<T, L> getChild(Trie.Node<T, L> node, Trie.Remaining has);

    /**
     * Called when a node is exited.
     * Returns the parent with which to continue the traversal.
     */
    abstract Trie.Node<T, L> exitNodeAndReturnParent(Trie.Node<T, L> n);

    /**
     * Called to retrieve the content to be issued for a given node (e.g. content(), or a (path, content()) pair.
     */
    abstract V contentOf(Trie.Node<T, L> n);
}
