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
package org.apache.cassandra.io.sstable;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Reads keys from an SSTable.
 * <p/>
 * It is specific to SSTable format how the keys are read but in general the assumption is that it will read all the
 * keys in the order as they are placed in data file.
 * <p/>
 * After creating it, it should be at the first key. Unless the SSTable is empty, {@link #key()},
 * {@link #keyPositionForSecondaryIndex()} and {@link #dataPosition()} should return approriate values. If there is
 * no data, {@link #isExhausted()} returns {@code true}. In order to move to the next key, {@link #advance()} should be
 * called. It returns {@code true} if the reader moved to the next key; otherwise, there is no more data to read and
 * the reader is exhausted. When the reader is exhausted, return values of {@link #key()},
 * {@link #keyPositionForSecondaryIndex()} and {@link #dataPosition()} are undefined.
 */
public interface KeyReader extends Closeable
{
    /**
     * Current key
     */
    ByteBuffer key();

    /**
     * Position in the component preferred for reading keys. This is specific to SSTable implementation
     */
    long keyPositionForSecondaryIndex();

    /**
     * Position in the data file where the associated content resides
     */
    long dataPosition();

    /**
     * Moves the iterator forward. Returns false if we reach EOF and there nothing more to read
     */
    boolean advance() throws IOException;

    /**
     * Returns true if we reach EOF
     */
    boolean isExhausted();

    /**
     * Resets the iterator to the initial position
     */
    void reset() throws IOException;

    /**
     * Closes the iterator quietly
     */
    @Override
    void close();
}