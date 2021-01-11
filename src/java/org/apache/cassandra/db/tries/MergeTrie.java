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

import com.google.common.collect.Iterables;

/**
 * A merged view of two tries.
 */
class MergeTrie<T> extends Trie<T>
{
    /**
     * Transition value used to indicate a transition is not present. Must be greater than all valid transition values
     * (0-0xFF).
     */
    public static final int NOT_PRESENT = 0x100;

    private final MergeResolver<T> resolver;
    protected final Trie<T> t1;
    protected final Trie<T> t2;

    MergeTrie(MergeResolver<T> resolver, Trie<T> t1, Trie<T> t2)
    {
        this.resolver = resolver;
        this.t1 = t1;
        this.t2 = t2;
    }

    @Override
    public <L> Node<T, L> root()
    {
        return makeNode(resolver, t1.root(), t2.root());
    }

    private static <T, L> Node<T, L> makeNode(MergeResolver<T> resolver, Node<T, L> child1, Node<T, L> child2)
    {
        if (child1 != null && child2 != null)
            return new MergeNode<>(resolver, child1, child2);

        if (child1 != null)
            return child1;

        if (child2 != null)
            return child2;

        return null;
    }

    static class MergeNode<T, L> extends Node<T, L>
    {
        private final MergeResolver<T> resolver;
        final Node<T, L> n1;
        final Node<T, L> n2;
        int b1;
        int b2;

        MergeNode(MergeResolver<T> resolver, Node<T, L> n1, Node<T, L> n2)
        {
            // Both children have the same parent link (passed during getCurrentChild). Use either as ours.
            super(n1.parentLink);
            assert n2.parentLink == n1.parentLink;
            this.resolver = resolver;
            this.n1 = n1;
            this.n2 = n2;
        }

        private Remaining makeState(Remaining has1, Remaining has2)
        {
            Remaining result;
            if (has1 != null)
            {
                b1 = n1.currentTransition;
                result = Remaining.MULTIPLE;
            }
            else
            {
                b1 = NOT_PRESENT;
                result = has2;
            }
            currentTransition = b1;
            if (has2 != null)
            {
                b2 = n2.currentTransition;
                if (b2 < b1)
                    currentTransition = b2;
                else if (b1 == b2 && has1 == Remaining.ONE && has2 == Remaining.ONE)
                    result = Remaining.ONE;
            }
            else
            {
                b2 = NOT_PRESENT;
                result = has1;
            }
            return result;
        }

        public Remaining startIteration()
        {
            return makeState(n1.startIteration(), n2.startIteration());
        }

        public Remaining advanceIteration()
        {
            int prevb1 = b1;
            int prevb2 = b2;
            // We must advance the state of the source with the smaller transition byte.
            // If their transition bytes are equal, we advance both.
            if (prevb1 <= prevb2)
            {
                boolean has = n1.advanceIteration() != null;
                b1 = has ? n1.currentTransition : NOT_PRESENT;
            }
            if (prevb1 >= prevb2)
            {
                boolean has = n2.advanceIteration() != null;
                b2 = has ? n2.currentTransition : NOT_PRESENT;
            }
            currentTransition = Math.min(b1, b2);
            return b1 < NOT_PRESENT || b2 < NOT_PRESENT ? Remaining.MULTIPLE : null;
        }

        public Node<T, L> getCurrentChild(L parent)
        {
            Node<T, L> child1 = null;
            Node<T, L> child2 = null;

            if (b1 <= b2)
                child1 = n1.getCurrentChild(parent);
            if (b1 >= b2)
                child2 = n2.getCurrentChild(parent);

            return makeNode(resolver, child1, child2);
        }

        public T content()
        {
            T mc = n2.content();
            T nc = n1.content();
            if (mc == null)
                return nc;
            else if (nc == null)
                return mc;
            else
                return resolver.resolve(nc, mc);
        }
    }

    /**
     * Special instance for sources that are guaranteed (by the caller) distinct. The main difference is that we can
     * form unordered value list by concatenating sources.
     */
    static class Distinct<T> extends MergeTrie<T>
    {
        Distinct(Trie<T> input1, Trie<T> input2)
        {
            super(throwingResolver(), input1, input2);
        }

        @Override
        public Iterable<T> valuesUnordered()
        {
            return Iterables.concat(t1.valuesUnordered(), t2.valuesUnordered());
        }
    }
}
