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
package org.apache.cassandra.io.sstable.format.bti;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Iterator over the partitions of an sstable used for scrubbing.
 * <p>
 * The difference between this and {@link PartitionIterator} is that this only uses information present in the index file
 * and does not try to read keys of the data file (for the trie index format), thus {@link #key()} can be null.
 * <p>
 * Starts advanced to a position, {@link #advance()} is to be used to go to next, and iteration completes when
 * {@link #dataPosition()} == -1.
 */
public interface ScrubPartitionIterator extends Closeable
{
    /**
     * Serialized partition key or {@code null} if the iterator reached the end of the index or if the key may not
     * be fully retrieved from the index file.
     */
    ByteBuffer key();

    /**
     * Key position in data file or -1 if the iterator reached the end of the index.
     */
    long dataPosition();

    /**
     * Move to the next position in the index file.
     */
    void advance() throws IOException;

    boolean isExhausted();

    void close();
}
