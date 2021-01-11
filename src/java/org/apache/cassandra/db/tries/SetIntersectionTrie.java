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
 * The intersection of a trie with the given set.
 */
class SetIntersectionTrie<T> extends Trie<T>
{
    private final Trie<T> trie;
    private final TrieSet intersectingSet;

    SetIntersectionTrie(Trie<T> trie, TrieSet intersectingSet)
    {
        this.trie = trie;
        this.intersectingSet = intersectingSet;
    }

    @Override
    public <L> Node<T, L> root()
    {
        TrieSet.SetNode sRoot = intersectingSet.root();
        if (sRoot == null)
            return null;

        Node<T, L> tRoot = trie.root();
        if (sRoot == TrieSet.FULL)
            return tRoot;
        if (tRoot == null)
            return null;

        return new IntersectionNode<>(tRoot, sRoot);
    }

    static class IntersectionNode<T, L> extends Node<T, L>
    {
        final Node<T, L> tNode;
        final TrieSet.SetNode sNode;

        public IntersectionNode(Node<T, L> tNode, TrieSet.SetNode sNode)
        {
            super(tNode.parentLink);
            this.tNode = tNode;
            this.sNode = sNode;
        }

        public Remaining startIteration()
        {
            boolean sHas = sNode.startIteration();
            if (!sHas)
                return null;

            return advanceToIntersection(tNode.startIteration());
        }

        public Remaining advanceIteration()
        {
            boolean sHas = sNode.advanceIteration();
            if (!sHas)
                return null;
            return advanceToIntersection(tNode.advanceIteration());
        }

        public Remaining advanceToIntersection(Remaining tHas)
        {
            boolean sHas;
            if (tHas == null)
                return null;
            int sByte = sNode.currentTransition();
            int tByte = tNode.currentTransition;

            while (tByte != sByte)
            {
                if (tByte < sByte)
                {
                    tHas = tNode.advanceIteration();
                    if (tHas == null)
                        return null;
                    tByte = tNode.currentTransition;
                }
                else // (tByte > sByte)
                {
                    sHas = sNode.advanceIteration();
                    if (!sHas)
                        return null;
                    sByte = sNode.currentTransition();
                }
            }
            currentTransition = sByte;
            return tHas;    // ONE or MULTIPLE
        }

        public Node<T, L> getCurrentChild(L parent)
        {
            TrieSet.SetNode receivedSetNode = sNode.getCurrentChild();

            if (receivedSetNode == null)
                return null;    // branch is completely outside the set

            Node<T, L> nn = tNode.getCurrentChild(parent);

            if (nn == null)
                return null;

            if (receivedSetNode == TrieSet.FULL)
                return nn;     // Branch is fully covered, we no longer need to augment nodes there.

            return new IntersectionNode<>(nn, receivedSetNode);
        }

        public T content()
        {
            if (sNode.inSet())
                return tNode.content();
            return null;
        }
    }
}
