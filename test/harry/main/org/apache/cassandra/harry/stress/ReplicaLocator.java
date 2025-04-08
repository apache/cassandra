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

package org.apache.cassandra.harry.stress;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TypeCodec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.gen.rng.SeedableEntropySource;

import static org.apache.cassandra.harry.SchemaSpec.forKeys;
import static org.apache.cassandra.harry.util.ByteUtils.putShortLength;

public class ReplicaLocator
{
    @SuppressWarnings("unchecked")
    public static Set<Host> getReplicaHosts(Session session, String keyspace, String table, Object[] pk)
    {
        Metadata metadata = session.getCluster().getMetadata();
        CodecRegistry codecRegistry = session.getCluster().getConfiguration().getCodecRegistry();
        ProtocolVersion protocolVersion = session.getCluster().getConfiguration()
                                                 .getProtocolOptions().getProtocolVersion();

        KeyspaceMetadata ksm = metadata.getKeyspace(keyspace);
        if (ksm == null)
            throw new IllegalArgumentException(String.format("Keyspace not found: %s", keyspace));

        TableMetadata tm = ksm.getTable(table);
        if (tm == null)
            throw new IllegalArgumentException(String.format("Table not found: %s.%s", keyspace, table));

        List<ColumnMetadata> pkColumns = tm.getPartitionKey();
        if (pk.length != pkColumns.size())
            throw new IllegalArgumentException(String.format("Expected %d partition key values but got %d",
                                                             pkColumns.size(), pk.length));

        ByteBuffer[] components = new ByteBuffer[pk.length];
        for (int i = 0; i < pk.length; i++)
        {
            DataType type = pkColumns.get(i).getType();
            TypeCodec codec = codecRegistry.codecFor(type);
            components[i] = codec.serialize(pk[i], protocolVersion);
        }

        ByteBuffer routingKey = compose(components);

        return metadata.getReplicas(keyspace, routingKey);
    }

    static ByteBuffer compose(ByteBuffer... buffers) {
        if (buffers.length == 1) {
            return buffers[0];
        } else {
            int totalLength = 0;

            for(ByteBuffer bb : buffers) {
                totalLength += 2 + bb.remaining() + 1;
            }

            ByteBuffer out = ByteBuffer.allocate(totalLength);

            for(ByteBuffer buffer : buffers) {
                ByteBuffer bb = buffer.duplicate();
                putShortLength(out, bb.remaining());
                out.put(bb);
                out.put((byte)0);
            }

            out.flip();
            return out;
        }
    }

    public static List<InetSocketAddress> getReplicas(Session session, SchemaSpec schema, long pd)
    {
       Object[] pk = SeedableEntropySource.computeWithSeed(pd, forKeys(schema.partitionKeys)::generate);
       return getReplicas(session, schema.keyspace, schema.table, pk);

    }
    public static List<InetSocketAddress> getReplicas(Session session, String keyspace, String table, Object[] pk)
    {
        Set<Host> replicas = getReplicaHosts(session, keyspace, table, pk);
        List<InetSocketAddress> addresses = new ArrayList<>(replicas.size());
        for (Host host : replicas)
            addresses.add(host.getBroadcastSocketAddress());
        return addresses;
    }
}
