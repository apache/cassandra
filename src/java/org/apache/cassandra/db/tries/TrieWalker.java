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
 * Utility class for performing some types of walks over the trie, where the result can be used as a
 * CompletableFuture.
 * See TrieDumper for sample usage.
 */
public interface TrieWalker<T, V>
{
    /**
     * Called when entering a node of the trie.
     *
     * @param incomingTransition the transition that led here, -1 if this is the root.
     */
    void onNodeEntry(int incomingTransition, T content);

    /**
     * Called when leaving a node of the trie, that is after having exited its last children.
     */
    void onNodeExit();

    /**
     * The final value of the trie walk.
     * <p>
     * This is called on completion of the walk (after calling {@link #onNodeExit} on the root node) to obtain the
     * final outcome of the walk.
     * <p>
     * Note: the type parameter L must be equal to {@code Trie.Node<T, L>}. There is no way to specify such recursive
     * types in Java, but it does get inferred correctly in calls to this method.
     *
     * @return the final outcome of the walk.
     */
    V completion();

    public static <T, V, L extends Trie.Node<T, L>> V process(TrieWalker<T, V> walker, Trie<T> trie)
    {
        Trie.Node<T, L> current = trie.root();
        if (current == null)
            return walker.completion();

        walker.onNodeEntry(-1, current.content());

        Trie.Remaining has = current.startIteration();

        while (true)
        {
            if (has != null)
            {
                // We have a transition, get child to descend into
                Trie.Node<T, L> child = current.getCurrentChild((L) current);
                if (child == null)
                {
                    // no child, get next
                    has = current.advanceIteration();
                }
                else
                {
                    walker.onNodeEntry(current.currentTransition, child.content());

                    // We have a new child. Move to it
                    current = child;
                    has = child.startIteration();
                }
            }
            else
            {
                // There are no more children. Ascend to the parent state to continue walk.
                walker.onNodeExit();
                current = current.parentLink;
                if (current == null)
                {
                    // We've reached back the root, our walk is finished
                    return walker.completion();
                }
                has = current.advanceIteration();
            }
        }
    }
}
