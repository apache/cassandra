/**
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
package org.apache.cassandra.db.context;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.Allocator;

/**
 * An opaque commutative context.
 *
 * Maintains a ByteBuffer context that represents a partitioned commutative value.
 */
public interface IContext
{
    public static enum ContextRelationship
    {
        EQUAL,
        GREATER_THAN,
        LESS_THAN,
        DISJOINT
    };

    /**
     * Determine the relationship between two contexts.
     *
     * EQUAL:        Equal set of nodes and every count is equal.
     * GREATER_THAN: Superset of nodes and every count is equal or greater than its corollary.
     * LESS_THAN:    Subset of nodes and every count is equal or less than its corollary.
     * DISJOINT:     Node sets are not equal and/or counts are not all greater or less than.
     *
     * @param left
     *            context.
     * @param right
     *            context.
     * @return the ContextRelationship between the contexts.
     */
    public ContextRelationship diff(ByteBuffer left, ByteBuffer right);

    /**
     * Return a context w/ an aggregated count for each node id.
     *
     * @param left
     *            context.
     * @param right
     *            context.
     * @param allocator
     *            an allocator to allocate the new context from.
     */
    public ByteBuffer merge(ByteBuffer left, ByteBuffer right, Allocator allocator);

    /**
     * Human-readable String from context.
     *
     * @param context
     *            context.
     * @return a human-readable String of the context.
     */
    public String toString(ByteBuffer context);
}
