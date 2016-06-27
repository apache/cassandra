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
import java.util.Objects;

import org.apache.cassandra.io.compress.BufferType;

/**
 * SequentialWriter option
 */
public class SequentialWriterOption
{
    /**
     * Default write option.
     *
     * <ul>
     *   <li>buffer size: 64 KB
     *   <li>buffer type: on heap
     *   <li>trickle fsync: false
     *   <li>trickle fsync byte interval: 10 MB
     *   <li>finish on close: false
     * </ul>
     */
    public static final SequentialWriterOption DEFAULT = SequentialWriterOption.newBuilder().build();

    private final int bufferSize;
    private final BufferType bufferType;
    private final boolean trickleFsync;
    private final int trickleFsyncByteInterval;
    private final boolean finishOnClose;

    private SequentialWriterOption(int bufferSize,
                                   BufferType bufferType,
                                   boolean trickleFsync,
                                   int trickleFsyncByteInterval,
                                   boolean finishOnClose)
    {
        this.bufferSize = bufferSize;
        this.bufferType = bufferType;
        this.trickleFsync = trickleFsync;
        this.trickleFsyncByteInterval = trickleFsyncByteInterval;
        this.finishOnClose = finishOnClose;
    }

    public static Builder newBuilder()
    {
        return new Builder();
    }

    public int bufferSize()
    {
        return bufferSize;
    }

    public BufferType bufferType()
    {
        return bufferType;
    }

    public boolean trickleFsync()
    {
        return trickleFsync;
    }

    public int trickleFsyncByteInterval()
    {
        return trickleFsyncByteInterval;
    }

    public boolean finishOnClose()
    {
        return finishOnClose;
    }

    /**
     * Allocate buffer using set buffer type and buffer size.
     *
     * @return allocated ByteBuffer
     */
    public ByteBuffer allocateBuffer()
    {
        return bufferType.allocate(bufferSize);
    }

    public static class Builder
    {
        /* default buffer size: 64k */
        private int bufferSize = 64 * 1024;
        /* default buffer type: on heap */
        private BufferType bufferType = BufferType.ON_HEAP;
        /* default: no trickle fsync */
        private boolean trickleFsync = false;
        /* default tricle fsync byte interval: 10MB */
        private int trickleFsyncByteInterval = 10 * 1024 * 1024;
        private boolean finishOnClose = false;

        /* construct throguh SequentialWriteOption.newBuilder */
        private Builder() {}

        public SequentialWriterOption build()
        {
            return new SequentialWriterOption(bufferSize, bufferType, trickleFsync,
                                   trickleFsyncByteInterval, finishOnClose);
        }

        public Builder bufferSize(int bufferSize)
        {
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder bufferType(BufferType bufferType)
        {
            this.bufferType = Objects.requireNonNull(bufferType);
            return this;
        }

        public Builder trickleFsync(boolean trickleFsync)
        {
            this.trickleFsync = trickleFsync;
            return this;
        }

        public Builder trickleFsyncByteInterval(int trickleFsyncByteInterval)
        {
            this.trickleFsyncByteInterval = trickleFsyncByteInterval;
            return this;
        }

        public Builder finishOnClose(boolean finishOnClose)
        {
            this.finishOnClose = finishOnClose;
            return this;
        }
    }
}
