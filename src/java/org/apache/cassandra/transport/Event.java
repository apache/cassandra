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

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

public abstract class Event
{
    public enum Type { TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE }

    public final Type type;

    private Event(Type type)
    {
        this.type = type;
    }

    public static Event deserialize(ChannelBuffer cb)
    {
        switch (CBUtil.readEnumValue(Type.class, cb))
        {
            case TOPOLOGY_CHANGE:
                return TopologyChange.deserializeEvent(cb);
            case STATUS_CHANGE:
                return StatusChange.deserializeEvent(cb);
            case SCHEMA_CHANGE:
                return SchemaChange.deserializeEvent(cb);
        }
        throw new AssertionError();
    }

    public ChannelBuffer serialize()
    {
        return ChannelBuffers.wrappedBuffer(CBUtil.enumValueToCB(type), serializeEvent());
    }

    protected abstract ChannelBuffer serializeEvent();

    public static class TopologyChange extends Event
    {
        public enum Change { NEW_NODE, REMOVED_NODE, MOVED_NODE }

        public final Change change;
        public final InetSocketAddress node;

        private TopologyChange(Change change, InetSocketAddress node)
        {
            super(Type.TOPOLOGY_CHANGE);
            this.change = change;
            this.node = node;
        }

        public static TopologyChange newNode(InetAddress host, int port)
        {
            return new TopologyChange(Change.NEW_NODE, new InetSocketAddress(host, port));
        }

        public static TopologyChange removedNode(InetAddress host, int port)
        {
            return new TopologyChange(Change.REMOVED_NODE, new InetSocketAddress(host, port));
        }

        public static TopologyChange movedNode(InetAddress host, int port)
        {
            return new TopologyChange(Change.MOVED_NODE, new InetSocketAddress(host, port));
        }

        // Assumes the type has already by been deserialized
        private static TopologyChange deserializeEvent(ChannelBuffer cb)
        {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            InetSocketAddress node = CBUtil.readInet(cb);
            return new TopologyChange(change, node);
        }

        protected ChannelBuffer serializeEvent()
        {
            return ChannelBuffers.wrappedBuffer(CBUtil.enumValueToCB(change), CBUtil.inetToCB(node));
        }

        @Override
        public String toString()
        {
            return change + " " + node;
        }
    }

    public static class StatusChange extends Event
    {
        public enum Status { UP, DOWN }

        public final Status status;
        public final InetSocketAddress node;

        private StatusChange(Status status, InetSocketAddress node)
        {
            super(Type.STATUS_CHANGE);
            this.status = status;
            this.node = node;
        }

        public static StatusChange nodeUp(InetAddress host, int port)
        {
            return new StatusChange(Status.UP, new InetSocketAddress(host, port));
        }

        public static StatusChange nodeDown(InetAddress host, int port)
        {
            return new StatusChange(Status.DOWN, new InetSocketAddress(host, port));
        }

        // Assumes the type has already by been deserialized
        private static StatusChange deserializeEvent(ChannelBuffer cb)
        {
            Status status = CBUtil.readEnumValue(Status.class, cb);
            InetSocketAddress node = CBUtil.readInet(cb);
            return new StatusChange(status, node);
        }

        protected ChannelBuffer serializeEvent()
        {
            return ChannelBuffers.wrappedBuffer(CBUtil.enumValueToCB(status), CBUtil.inetToCB(node));
        }

        @Override
        public String toString()
        {
            return status + " " + node;
        }
    }

    public static class SchemaChange extends Event
    {
        public enum Change { CREATED, UPDATED, DROPPED }

        public final Change change;
        public final String keyspace;
        public final String table;

        public SchemaChange(Change change, String keyspace, String table)
        {
            super(Type.SCHEMA_CHANGE);
            this.change = change;
            this.keyspace = keyspace;
            this.table = table;
        }

        public SchemaChange(Change change, String keyspace)
        {
            this(change, keyspace, "");
        }

        // Assumes the type has already by been deserialized
        private static SchemaChange deserializeEvent(ChannelBuffer cb)
        {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            String keyspace = CBUtil.readString(cb);
            String table = CBUtil.readString(cb);
            return new SchemaChange(change, keyspace, table);
        }

        protected ChannelBuffer serializeEvent()
        {
            return ChannelBuffers.wrappedBuffer(CBUtil.enumValueToCB(change),
                                                CBUtil.stringToCB(keyspace),
                                                CBUtil.stringToCB(table));
        }

        @Override
        public String toString()
        {
            return change + " " + keyspace + (table.isEmpty() ? "" : "." + table);
        }
    }
}
