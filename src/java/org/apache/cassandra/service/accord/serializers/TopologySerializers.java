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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;

import accord.local.Node;
import accord.messages.TxnRequest;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class TopologySerializers
{
    private TopologySerializers() {}

    public static final NodeIdSerializer nodeId = new NodeIdSerializer();
    public static class NodeIdSerializer implements IVersionedSerializer<Node.Id>
    {
        private NodeIdSerializer() {}

        @Override
        public void serialize(Node.Id id, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(id.id);
        }

        public <V> int serialize(Node.Id id, V dst, ValueAccessor<V> accessor, int offset)
        {
            return accessor.putLong(dst, offset, id.id);
        }

        @Override
        public Node.Id deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Node.Id(in.readLong());
        }

        public <V> Node.Id deserialize(V src, ValueAccessor<V> accessor, int offset)
        {
            return new Node.Id(accessor.getLong(src, offset));
        }

        @Override
        public long serializedSize(Node.Id id, int version)
        {
            return serializedSize();
        }

        public int serializedSize()
        {
            return TypeSizes.LONG_SIZE;  // id.id
        }
    };
}
