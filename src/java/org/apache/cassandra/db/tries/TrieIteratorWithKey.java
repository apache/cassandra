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

import java.util.AbstractMap;
import java.util.Arrays;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Convertor of trie content to flow, with information about he paths used to reach the content node.
 * Descendants need to implement {@link TrieIterator#contentOf(Trie.Node)}; when the method is called the first
 * {@link #ppos} bytes of {@link #path} will be filled with the path used to reach the node.
 */
public abstract class TrieIteratorWithKey<T, V>
extends TrieIterator<T, TrieIteratorWithKey.NodeWithPosition<T>, V>
        implements Trie.TransitionsReceiver
{
    byte[] path = new byte[256];
    int ppos = 0;
    NodeWithPosition<T> currentParentLink = (NodeWithPosition<T>) NO_LINK;

    static final NodeWithPosition<Object> NO_LINK = new NodeWithPosition<>(-1, null);

    static class NodeWithPosition<T>
    {
        final int ppos;
        final Trie.Node<T, NodeWithPosition<T>> node;

        NodeWithPosition(int ppos, Trie.Node<T, NodeWithPosition<T>> node)
        {
            this.ppos = ppos;
            this.node = node;
        }
    }

    protected TrieIteratorWithKey(Trie<T> trie)
    {
        super(trie);
    }

    public void add(int t)
    {
        if (ppos == path.length)
            path = Arrays.copyOf(path, path.length * 2);
        path[ppos++] = (byte) t;
    }

    public void add(UnsafeBuffer b, int pos, int count)
    {
        if (ppos + count > path.length)
            path = Arrays.copyOf(path, Math.max(ppos + count + 16, path.length * 2));
        b.getBytes(pos, path, ppos, count);
        ppos += count;
    }

    Trie.Node<T, NodeWithPosition<T>> getChild(Trie.Node<T, NodeWithPosition<T>> node, Trie.Remaining has)
    {
        int currentPos = ppos;
        add(node.currentTransition);

        NodeWithPosition<T> parentLink;
        if (has == Trie.Remaining.ONE)
        {
            // As in TrieValuesIterator, when we are processing the last child of a node we can skip it when backtracking.
            parentLink = node.parentLink;
        }
        else
        {
            assert has != null;
            // Otherwise, we need to be returning to this node. Create a parentLink object if one doesn't yet exist,
            // saving the byte position in the path.
            if (currentParentLink.node != node)
            {
                assert currentParentLink.ppos < currentPos;
                currentParentLink = new NodeWithPosition<>(currentPos, node);
            }
            parentLink = currentParentLink;
        }

        Trie.Node<T, NodeWithPosition<T>> child = node.getCurrentChild(parentLink);

        if (child != null)
            child = child.getUniqueDescendant(parentLink, this);

        if (child == null)
            ppos = parentLink != null ? parentLink.ppos : 0; // restore state as we won't get an exitNodeAndReturnParent call.

        return child;
    }

    Trie.Node<T, NodeWithPosition<T>> exitNodeAndReturnParent(Trie.Node<T, NodeWithPosition<T>> n)
    {
        NodeWithPosition<T> parentLink = n.parentLink;
        if (parentLink == null)
            return null;
        else
        {
            ppos = parentLink.ppos;
            currentParentLink = parentLink;
            return parentLink.node;
        }
    }

    static <T> java.util.Map.Entry<ByteComparable, T> toEntry(T content, byte[] bytes, int byteLength)
    {
        ByteComparable b = ByteComparable.fixedLength(Arrays.copyOf(bytes, byteLength));
        return new AbstractMap.SimpleImmutableEntry<>(b, content);
    }
}
