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

package org.apache.cassandra.distributed.impl;

import java.net.InetSocketAddress;

import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.shared.NetworkTopology;

// a container for simplifying the method signature for per-instance message handling/delivery
public class MessageImpl implements IMessage
{
    public final int verb;
    public final byte[] bytes;
    public final long id;
    public final int version;
    public final InetSocketAddress from;

    public MessageImpl(int verb, byte[] bytes, long id, int version, InetSocketAddress from)
    {
        this.verb = verb;
        this.bytes = bytes;
        this.id = id;
        this.version = version;
        this.from = from;
    }

    public int verb()
    {
        return verb;
    }

    public byte[] bytes()
    {
        return bytes;
    }

    public int id()
    {
        return (int) id;
    }

    public int version()
    {
        return version;
    }

    public InetSocketAddress from()
    {
        return from;
    }
}

