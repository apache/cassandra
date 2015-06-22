package org.apache.cassandra.io.util;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.io.compress.BufferType;

public class ThrottledReader extends RandomAccessReader
{
    private final RateLimiter limiter;

    protected ThrottledReader(ChannelProxy channel, int bufferSize, long overrideLength, RateLimiter limiter)
    {
        super(channel, bufferSize, overrideLength, BufferType.OFF_HEAP);
        this.limiter = limiter;
    }

    protected void reBuffer()
    {
        limiter.acquire(buffer.capacity());
        super.reBuffer();
    }

    public static ThrottledReader open(ChannelProxy channel, int bufferSize, long overrideLength, RateLimiter limiter)
    {
        return new ThrottledReader(channel, bufferSize, overrideLength, limiter);
    }
}
