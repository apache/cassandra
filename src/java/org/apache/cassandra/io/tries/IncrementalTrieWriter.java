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

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Common interface for incremental trie writers. Incremental writers take sorted input to construct a trie file while
 * buffering only limited amount of data.
 * The writing itself is done by some node serializer passed on construction time.
 * <p>
 * See {@code org/apache/cassandra/io/sstable/format/bti/BtiFormat.md} for a description of the mechanisms of writing
 * and reading an on-disk trie.
 */
public interface IncrementalTrieWriter<VALUE> extends AutoCloseable
{
    /**
     * Add an entry to the trie with the associated value.
     */
    void add(ByteComparable next, VALUE value) throws IOException;

    /**
     * Return the number of added entries.
     */
    long count();

    /**
     * Complete the process and return the position in the file of the root node.
     */
    long complete() throws IOException;

    void reset();

    void close();

    /**
     * Make a temporary in-memory representation of the unwritten nodes that covers everything added to the trie until
     * this point. The object returned represents a "tail" for the file that needs to be attached at the "cutoff" point
     * to the file (using e.g. TailOverridingRebufferer).
     */
    PartialTail makePartialRoot() throws IOException;


    interface PartialTail
    {
        /** Position of the root of the partial representation. Resides in the tail buffer. */ 
        long root();
        /** Number of keys written */
        long count();
        /** Cutoff point. Positions lower that this are to be read from the file; higher ones from the tail buffer. */
        long cutoff();
        /** Buffer containing in-memory representation of the tail. */
        ByteBuffer tail();
    }

    /**
     * Construct a suitable trie writer.
     */
    static <VALUE> IncrementalTrieWriter<VALUE> open(TrieSerializer<VALUE, ? super DataOutputPlus> trieSerializer, DataOutputPlus dest)
    {
        return new IncrementalDeepTrieWriterPageAware<>(trieSerializer, dest);
    }
}