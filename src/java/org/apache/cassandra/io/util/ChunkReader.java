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

package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import org.apache.cassandra.io.compress.BufferType;

/**
 * RandomFileReader component that reads data from a file into a provided buffer and may have requirements over the
 * size and alignment of reads.
 * A caching or buffer-managing rebufferer will reference one of these to do the actual reading.
 * Note: Implementations of this interface must be thread-safe!
 */
public interface ChunkReader extends RebuffererFactory
{
    /**
     * Read the chunk at the given position, attempting to fill the capacity of the given buffer.
     * The filled buffer must be positioned at 0, with limit set at the size of the available data.
     * The source may have requirements for the positioning and/or size of the buffer (e.g. chunk-aligned and
     * chunk-sized). These must be satisfied by the caller. 
     */
    void readChunk(long position, ByteBuffer buffer);

    /**
     * Buffer size required for this rebufferer. Must be power of 2 if alignment is required.
     */
    int chunkSize();

    /**
     * Specifies type of buffer the caller should attempt to give.
     * This is not guaranteed to be fulfilled.
     */
    BufferType preferredBufferType();
}
