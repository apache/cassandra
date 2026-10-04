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
package org.apache.cassandra.index.sai.memory;

import org.apache.cassandra.index.sai.utils.PrimaryKey;
import org.apache.cassandra.index.sai.utils.PrimaryKeys;

/**
 * Trie node payload for {@link TrieMemoryIndex} when the SAI prefix feature is enabled.
 * <p>
 * Holds two sets of {@link PrimaryKeys}:
 * <ul>
 *   <li>{@link #exact} — primary keys of rows whose indexed term is exactly the term at this node
 *       (populated via {@link org.apache.cassandra.db.tries.InMemoryTrie.UpsertTransformer#apply}).</li>
 *   <li>{@link #prefix} — primary keys of rows in this node's subtree, accumulated at eligible
 *       intermediate depths via {@link org.apache.cassandra.db.tries.InMemoryTrie.UpsertTransformer#applyIntermediate}.
 *       This set is only populated when {@code prefixAtDepth.test(depth)} is true, following
 *       the same eligibility rule as {@link org.apache.cassandra.config.CassandraRelevantProperties#SAI_POSTINGS_SKIP}.
 *       </li>
 * </ul>
 *
 * At flush time the {@code prefix} set is converted to on-disk combined postings sections.
 * During in-memory search only {@code exact} is consulted.
 */
public class SectionedPrimaryKeys
{
    private final PrimaryKeys exact = new PrimaryKeys();
    private PrimaryKeys prefix;  // lazily allocated; null when no intermediate accumulation

    /** Add a primary key to the exact section (terminal-node insert). */
    public long addExact(PrimaryKey key)
    {
        return exact.add(key);
    }

    /** Add a primary key to the prefix section (intermediate-node insert). */
    public long addPrefix(PrimaryKey key)
    {
        if (prefix == null)
            prefix = new PrimaryKeys();
        return prefix.add(key);
    }

    /** Primary keys whose indexed term exactly equals this trie node's key. Never null. */
    public PrimaryKeys exact()
    {
        return exact;
    }

    /**
     * Primary keys accumulated from this subtree for the prefix combined section.
     * Returns an empty {@link PrimaryKeys} when no intermediate accumulation has occurred.
     */
    public PrimaryKeys prefix()
    {
        return prefix == null ? PrimaryKeys.EMPTY : prefix;
    }

    /** Returns true if neither exact nor prefix contains any keys. */
    public boolean isEmpty()
    {
        return exact.isEmpty() && (prefix == null || prefix.isEmpty());
    }

    /** Approximate heap overhead of this object and its contents. */
    public long unsharedHeapSize()
    {
        return exact.unsharedHeapSize() + (prefix == null ? 0 : prefix.unsharedHeapSize());
    }
}
