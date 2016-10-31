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
import java.util.Iterator;
import java.util.List;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

public abstract class Event
{
    public enum Type
    {
        TOPOLOGY_CHANGE(ProtocolVersion.V3),
        STATUS_CHANGE(ProtocolVersion.V3),
        SCHEMA_CHANGE(ProtocolVersion.V3),
        TRACE_COMPLETE(ProtocolVersion.V4);

        public final ProtocolVersion minimumVersion;

        Type(ProtocolVersion minimumVersion)
        {
            this.minimumVersion = minimumVersion;
        }
    }

    public final Type type;

    private Event(Type type)
    {
        this.type = type;
    }

    public static Event deserialize(ByteBuf cb, ProtocolVersion version)
    {
        Type eventType = CBUtil.readEnumValue(Type.class, cb);
        if (eventType.minimumVersion.isGreaterThan(version))
            throw new ProtocolException("Event " + eventType.name() + " not valid for protocol version " + version);
        switch (eventType)
        {
            case TOPOLOGY_CHANGE:
                return TopologyChange.deserializeEvent(cb, version);
            case STATUS_CHANGE:
                return StatusChange.deserializeEvent(cb, version);
            case SCHEMA_CHANGE:
                return SchemaChange.deserializeEvent(cb, version);
        }
        throw new AssertionError();
    }

    public void serialize(ByteBuf dest, ProtocolVersion version)
    {
        if (type.minimumVersion.isGreaterThan(version))
            throw new ProtocolException("Event " + type.name() + " not valid for protocol version " + version);
        CBUtil.writeEnumValue(type, dest);
        serializeEvent(dest, version);
    }

    public int serializedSize(ProtocolVersion version)
    {
        return CBUtil.sizeOfEnumValue(type) + eventSerializedSize(version);
    }

    protected abstract void serializeEvent(ByteBuf dest, ProtocolVersion version);
    protected abstract int eventSerializedSize(ProtocolVersion version);

    public static abstract class NodeEvent extends Event
    {
        public final InetSocketAddress node;

        public InetAddress nodeAddress()
        {
            return node.getAddress();
        }

        private NodeEvent(Type type, InetSocketAddress node)
        {
            super(type);
            this.node = node;
        }
    }

    public static class TopologyChange extends NodeEvent
    {
        public enum Change { NEW_NODE, REMOVED_NODE, MOVED_NODE }

        public final Change change;

        private TopologyChange(Change change, InetSocketAddress node)
        {
            super(Type.TOPOLOGY_CHANGE, node);
            this.change = change;
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

        // Assumes the type has already been deserialized
        private static TopologyChange deserializeEvent(ByteBuf cb, ProtocolVersion version)
        {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            InetSocketAddress node = CBUtil.readInet(cb);
            return new TopologyChange(change, node);
        }

        protected void serializeEvent(ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeEnumValue(change, dest);
            CBUtil.writeInet(node, dest);
        }

        protected int eventSerializedSize(ProtocolVersion version)
        {
            return CBUtil.sizeOfEnumValue(change) + CBUtil.sizeOfInet(node);
        }

        @Override
        public String toString()
        {
            return change + " " + node;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(change, node);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof TopologyChange))
                return false;

            TopologyChange tpc = (TopologyChange)other;
            return Objects.equal(change, tpc.change)
                && Objects.equal(node, tpc.node);
        }
    }


    public static class StatusChange extends NodeEvent
    {
        public enum Status { UP, DOWN }

        public final Status status;

        private StatusChange(Status status, InetSocketAddress node)
        {
            super(Type.STATUS_CHANGE, node);
            this.status = status;
        }

        public static StatusChange nodeUp(InetAddress host, int port)
        {
            return new StatusChange(Status.UP, new InetSocketAddress(host, port));
        }

        public static StatusChange nodeDown(InetAddress host, int port)
        {
            return new StatusChange(Status.DOWN, new InetSocketAddress(host, port));
        }

        // Assumes the type has already been deserialized
        private static StatusChange deserializeEvent(ByteBuf cb, ProtocolVersion version)
        {
            Status status = CBUtil.readEnumValue(Status.class, cb);
            InetSocketAddress node = CBUtil.readInet(cb);
            return new StatusChange(status, node);
        }

        protected void serializeEvent(ByteBuf dest, ProtocolVersion version)
        {
            CBUtil.writeEnumValue(status, dest);
            CBUtil.writeInet(node, dest);
        }

        protected int eventSerializedSize(ProtocolVersion version)
        {
            return CBUtil.sizeOfEnumValue(status) + CBUtil.sizeOfInet(node);
        }

        @Override
        public String toString()
        {
            return status + " " + node;
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(status, node);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof StatusChange))
                return false;

            StatusChange stc = (StatusChange)other;
            return Objects.equal(status, stc.status)
                && Objects.equal(node, stc.node);
        }
    }

    public static class SchemaChange extends Event
    {
        public enum Change { CREATED, UPDATED, DROPPED }
        public enum Target { KEYSPACE, TABLE, TYPE, FUNCTION, AGGREGATE }

        public final Change change;
        public final Target target;
        public final String keyspace;
        public final String name;
        public final List<String> argTypes;

        public SchemaChange(Change change, Target target, String keyspace, String name, List<String> argTypes)
        {
            super(Type.SCHEMA_CHANGE);
            this.change = change;
            this.target = target;
            this.keyspace = keyspace;
            this.name = name;
            if (target != Target.KEYSPACE)
                assert this.name != null : "Table, type, function or aggregate name should be set for non-keyspace schema change events";
            this.argTypes = argTypes;
        }

        public SchemaChange(Change change, Target target, String keyspace, String name)
        {
            this(change, target, keyspace, name, null);
        }

        public SchemaChange(Change change, String keyspace)
        {
            this(change, Target.KEYSPACE, keyspace, null);
        }

        // Assumes the type has already been deserialized
        public static SchemaChange deserializeEvent(ByteBuf cb, ProtocolVersion version)
        {
            Change change = CBUtil.readEnumValue(Change.class, cb);
            if (version.isGreaterOrEqualTo(ProtocolVersion.V3))
            {
                Target target = CBUtil.readEnumValue(Target.class, cb);
                String keyspace = CBUtil.readString(cb);
                String tableOrType = target == Target.KEYSPACE ? null : CBUtil.readString(cb);
                List<String> argTypes = null;
                if (target == Target.FUNCTION || target == Target.AGGREGATE)
                    argTypes = CBUtil.readStringList(cb);

                return new SchemaChange(change, target, keyspace, tableOrType, argTypes);
            }
            else
            {
                String keyspace = CBUtil.readString(cb);
                String table = CBUtil.readString(cb);
                return new SchemaChange(change, table.isEmpty() ? Target.KEYSPACE : Target.TABLE, keyspace, table.isEmpty() ? null : table);
            }
        }

        public void serializeEvent(ByteBuf dest, ProtocolVersion version)
        {
            if (target == Target.FUNCTION || target == Target.AGGREGATE)
            {
                if (version.isGreaterOrEqualTo(ProtocolVersion.V4))
                {
                    // available since protocol version 4
                    CBUtil.writeEnumValue(change, dest);
                    CBUtil.writeEnumValue(target, dest);
                    CBUtil.writeString(keyspace, dest);
                    CBUtil.writeString(name, dest);
                    CBUtil.writeStringList(argTypes, dest);
                }
                else
                {
                    // not available in protocol versions < 4 - just say the keyspace was updated.
                    CBUtil.writeEnumValue(Change.UPDATED, dest);
                    if (version.isGreaterOrEqualTo(ProtocolVersion.V3))
                        CBUtil.writeEnumValue(Target.KEYSPACE, dest);
                    CBUtil.writeString(keyspace, dest);
                    CBUtil.writeString("", dest);
                }
                return;
            }

            if (version.isGreaterOrEqualTo(ProtocolVersion.V3))
            {
                CBUtil.writeEnumValue(change, dest);
                CBUtil.writeEnumValue(target, dest);
                CBUtil.writeString(keyspace, dest);
                if (target != Target.KEYSPACE)
                    CBUtil.writeString(name, dest);
            }
            else
            {
                if (target == Target.TYPE)
                {
                    // For the v1/v2 protocol, we have no way to represent type changes, so we simply say the keyspace
                    // was updated.  See CASSANDRA-7617.
                    CBUtil.writeEnumValue(Change.UPDATED, dest);
                    CBUtil.writeString(keyspace, dest);
                    CBUtil.writeString("", dest);
                }
                else
                {
                    CBUtil.writeEnumValue(change, dest);
                    CBUtil.writeString(keyspace, dest);
                    CBUtil.writeString(target == Target.KEYSPACE ? "" : name, dest);
                }
            }
        }

        public int eventSerializedSize(ProtocolVersion version)
        {
            if (target == Target.FUNCTION || target == Target.AGGREGATE)
            {
                if (version.isGreaterOrEqualTo(ProtocolVersion.V4))
                    return CBUtil.sizeOfEnumValue(change)
                               + CBUtil.sizeOfEnumValue(target)
                               + CBUtil.sizeOfString(keyspace)
                               + CBUtil.sizeOfString(name)
                               + CBUtil.sizeOfStringList(argTypes);
                if (version.isGreaterOrEqualTo(ProtocolVersion.V3))
                    return CBUtil.sizeOfEnumValue(Change.UPDATED)
                           + CBUtil.sizeOfEnumValue(Target.KEYSPACE)
                           + CBUtil.sizeOfString(keyspace);
                return CBUtil.sizeOfEnumValue(Change.UPDATED)
                       + CBUtil.sizeOfString(keyspace)
                       + CBUtil.sizeOfString("");
            }

            if (version.isGreaterOrEqualTo(ProtocolVersion.V3))
            {
                int size = CBUtil.sizeOfEnumValue(change)
                         + CBUtil.sizeOfEnumValue(target)
                         + CBUtil.sizeOfString(keyspace);

                if (target != Target.KEYSPACE)
                    size += CBUtil.sizeOfString(name);

                return size;
            }
            else
            {
                if (target == Target.TYPE)
                {
                    return CBUtil.sizeOfEnumValue(Change.UPDATED)
                         + CBUtil.sizeOfString(keyspace)
                         + CBUtil.sizeOfString("");
                }
                return CBUtil.sizeOfEnumValue(change)
                     + CBUtil.sizeOfString(keyspace)
                     + CBUtil.sizeOfString(target == Target.KEYSPACE ? "" : name);
            }
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder().append(change)
                                                  .append(' ').append(target)
                                                  .append(' ').append(keyspace);
            if (name != null)
                sb.append('.').append(name);
            if (argTypes != null)
            {
                sb.append(" (");
                for (Iterator<String> iter = argTypes.iterator(); iter.hasNext(); )
                {
                    sb.append(iter.next());
                    if (iter.hasNext())
                        sb.append(',');
                }
                sb.append(')');
            }
            return sb.toString();
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(change, target, keyspace, name, argTypes);
        }

        @Override
        public boolean equals(Object other)
        {
            if (!(other instanceof SchemaChange))
                return false;

            SchemaChange scc = (SchemaChange)other;
            return Objects.equal(change, scc.change)
                && Objects.equal(target, scc.target)
                && Objects.equal(keyspace, scc.keyspace)
                && Objects.equal(name, scc.name)
                && Objects.equal(argTypes, scc.argTypes);
        }
    }
}
