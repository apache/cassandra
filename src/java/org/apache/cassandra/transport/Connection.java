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
package org.apache.cassandra.transport;

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

public class Connection
{
    static final AttributeKey<Connection> attributeKey = AttributeKey.valueOf("CONN");

    private final Channel channel;
    private final ProtocolVersion version;
    private final Tracker tracker;

    private volatile FrameCompressor frameCompressor;
    private boolean throwOnOverload;

    public Connection(Channel channel, ProtocolVersion version, Tracker tracker)
    {
        this.channel = channel;
        this.version = version;
        this.tracker = tracker;

        tracker.addConnection(channel, this);
    }

    public void setCompressor(FrameCompressor compressor)
    {
        this.frameCompressor = compressor;
    }

    public FrameCompressor getCompressor()
    {
        return frameCompressor;
    }

    public void setThrowOnOverload(boolean throwOnOverload)
    {
        this.throwOnOverload = throwOnOverload;
    }

    public boolean isThrowOnOverload()
    {
        return throwOnOverload;
    }

    public Tracker getTracker()
    {
        return tracker;
    }

    public ProtocolVersion getVersion()
    {
        return version;
    }

    public Channel channel()
    {
        return channel;
    }

    public interface Factory
    {
        Connection newConnection(Channel channel, ProtocolVersion version);
    }

    public interface Tracker
    {
        void addConnection(Channel ch, Connection connection);
    }
}
