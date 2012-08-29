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
package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.cassandra.thrift.ColumnParent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
public class TraceState
{
    public final UUID sessionId;
    public final InetAddress coordinator;
    public final Stopwatch watch;
    public final ByteBuffer sessionIdBytes;

    public TraceState(InetAddress coordinator, UUID sessionId)
    {
        assert coordinator != null;
        assert sessionId != null;

        this.coordinator = coordinator;
        this.sessionId = sessionId;
        sessionIdBytes = ByteBufferUtil.bytes(sessionId);
        watch = new Stopwatch();
        watch.start();
    }

    public int elapsed()
    {
        long elapsed = watch.elapsedTime(TimeUnit.MICROSECONDS);
        return elapsed < Integer.MAX_VALUE ? (int) elapsed : Integer.MAX_VALUE;
    }
}
