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
package org.apache.cassandra.io.sstable.format;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Iterator over the partitions of an sstable.
 * <p>
 * The index iterator starts with a key/position ready. advance() should be used to move to the next key; iteration
 * completes when key() == null.
 */
public interface PartitionIndexIterator extends Closeable
{
    /**
     * Current key
     */
    public ByteBuffer key();

    /**
     * Position in the data file where the associated content resides
     */
    public long dataPosition();

    /**
     * Moves the iterator forward. Returns false if we reach EOF and there nothing more to read
     */
    public boolean advance() throws IOException;

    /**
     * Closes the iterator quietly
     */
    public void close();

    /**
     * Returns true if we reach EOF
     */
    boolean isExhausted();

    /**
     * Returns the current position in index file (which along with {@link #indexLength()}
     * can be used to track iteration progress
     */
    long indexPosition();

    /**
     * Sets the current position in index file
     */
    void indexPosition(long position) throws IOException;

    /**
     * Returns length of the index file
     */
    long indexLength();

    /**
     * Resets the iterator to the initial position
     */
    void reset() throws IOException;
}