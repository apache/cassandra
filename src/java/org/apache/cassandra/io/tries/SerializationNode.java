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
package org.apache.cassandra.io.tries;

/**
 * An abstraction of a node given to the trie serializer to write.
 */
public interface SerializationNode<VALUE>
{
    /**
     * The number of children of the node.
     */
    int childCount();

    /**
     * The payload of the node if the node has any associated, otherwise null.
     */
    VALUE payload();

    /**
     * The transition character for the child at position i. Must be an integer between 0 and 255.
     */
    int transition(int i);

    /**
     * Returns the distance between this node's position and the child at index i.
     * Given as a difference calculation to be able to handle two different types of calls:
     * - writing nodes where all children's positions are already completely determined
     * - sizing and writing branches within a page where we don't know where we'll actually place
     *   the nodes, but we know how far backward the child nodes will end up
     */
    long serializedPositionDelta(int i, long nodePosition);

    /**
     * Returns the furthest distance that needs to be written to store this node, i.e.
     *   min(serializedPositionDelta(i, nodePosition) for 0 <= i < childCount())
     * Given separately as the loop above can be inefficient (e.g. when children are not yet written).
     */
    long maxPositionDelta(long nodePosition);
}
