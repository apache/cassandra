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

package org.apache.cassandra.streaming;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import io.netty.channel.FileRegion;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Shared;

import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

@Shared(scope = SIMULATION, inner = INTERFACES)
public interface StreamingDataOutputPlus extends DataOutputPlus, Closeable
{
    interface BufferSupplier
    {
        /**
         * Request a buffer with at least the given capacity.
         * This method may only be invoked once, and the lifetime of buffer it returns will be managed
         * by the AsyncChannelOutputPlus it was created for.
         */
        ByteBuffer get(int capacity) throws IOException;
    }

    interface Write
    {
        /**
         * Write to a buffer, and flush its contents to the channel.
         * <p>
         * The lifetime of the buffer will be managed by the AsyncChannelOutputPlus you issue this Write to.
         * If the method exits successfully, the contents of the buffer will be written to the channel, otherwise
         * the buffer will be cleaned and the exception propagated to the caller.
         */
        void write(BufferSupplier supplier) throws IOException;
    }

    interface RateLimiter
    {
        void acquire(int bytes);

        boolean isRateLimited();
    }

    /**
     * Provide a lambda that can request a buffer of suitable size, then fill the buffer and have
     * that buffer written and flushed to the underlying channel, without having to handle buffer
     * allocation, lifetime or cleanup, including in case of exceptions.
     * <p>
     * Any exception thrown by the Write will be propagated to the caller, after any buffer is cleaned up.
     */
    int writeToChannel(Write write, RateLimiter limiter) throws IOException;

    /**
     * Writes all data in file channel to stream: <br>
     * * For zero-copy-streaming, 1MiB at a time, with at most 2MiB in flight at once. <br>
     * * For streaming with SSL, 64KiB at a time, with at most 32+64KiB (default low water mark + batch size) in flight. <br>
     * <p>
     * This method takes ownership of the provided {@link FileChannel}.
     * <p>
     * WARNING: this method blocks only for permission to write to the netty channel; it exits before
     * the {@link FileRegion}(zero-copy) or {@link ByteBuffer}(ssl) is flushed to the network.
     */
    long writeFileToChannel(FileChannel file, RateLimiter limiter) throws IOException;

    default void flush() throws IOException {}
}
