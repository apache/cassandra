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

/**
 * Convertor of trie contents to flow.
 *
 * Note: the type argument L must be equal to {@code Trie.Node<T, L>}, but we can't define such a recursive type in
 * Java. Using {@code <>} when instantiating works, but any subclasses will also need to declare this useless type
 * argument.
 */
class TrieValuesIterator<T, L extends Trie.Node<T, L>> extends TrieIterator<T, L, T>
{
    public TrieValuesIterator(Trie<T> trie)
    {
        super(trie);
    }

    Trie.Node<T, L> getChild(Trie.Node<T, L> node, Trie.Remaining has)
    {
        // If we know this is the last child for this node, we can just as well skip this node when backtracking,
        final L parentLink = has == Trie.Remaining.ONE ? node.parentLink : (L) node;

        Trie.Node<T, L> child = node.getCurrentChild(parentLink);

        // and as long as any child has single descendant, we don't need to backtrack to that either.
        if (child != null)
            child = child.getUniqueDescendant(parentLink, null);

        return child;
    }

    Trie.Node<T, L> exitNodeAndReturnParent(Trie.Node<T, L> n)
    {
        return n.parentLink;
    }

    T contentOf(Trie.Node<T, L> node)
    {
        return node.content();
    }
}
