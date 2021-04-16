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
package org.apache.cassandra.db.virtual;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.function.ToLongFunction;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.OutboundConnection;
import org.apache.cassandra.net.OutboundConnections;
import org.apache.cassandra.schema.TableMetadata;

public final class InternodeOutboundTable extends AbstractVirtualTable
{
    private static final String ADDRESS = "address";
    private static final String PORT = "port";
    private static final String DC = "dc";
    private static final String RACK = "rack";

    private static final String USING_BYTES = "using_bytes";
    private static final String USING_RESERVE_BYTES = "using_reserve_bytes";
    private static final String PENDING_COUNT = "pending_count";
    private static final String PENDING_BYTES = "pending_bytes";
    private static final String SENT_COUNT = "sent_count";
    private static final String SENT_BYTES = "sent_bytes";
    private static final String EXPIRED_COUNT = "expired_count";
    private static final String EXPIRED_BYTES = "expired_bytes";
    private static final String ERROR_COUNT = "error_count";
    private static final String ERROR_BYTES = "error_bytes";
    private static final String OVERLOAD_COUNT = "overload_count";
    private static final String OVERLOAD_BYTES = "overload_bytes";
    private static final String ACTIVE_CONNECTION_COUNT = "active_connections";
    private static final String CONNECTION_ATTEMPTS = "connection_attempts";
    private static final String SUCCESSFUL_CONNECTION_ATTEMPTS = "successful_connection_attempts";

    InternodeOutboundTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "internode_outbound")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(InetAddressType.instance, Int32Type.instance)))
                           .addPartitionKeyColumn(ADDRESS, InetAddressType.instance)
                           .addPartitionKeyColumn(PORT, Int32Type.instance)
                           .addClusteringColumn(DC, UTF8Type.instance)
                           .addClusteringColumn(RACK, UTF8Type.instance)
                           .addRegularColumn(USING_BYTES, LongType.instance)
                           .addRegularColumn(USING_RESERVE_BYTES, LongType.instance)
                           .addRegularColumn(PENDING_COUNT, LongType.instance)
                           .addRegularColumn(PENDING_BYTES, LongType.instance)
                           .addRegularColumn(SENT_COUNT, LongType.instance)
                           .addRegularColumn(SENT_BYTES, LongType.instance)
                           .addRegularColumn(EXPIRED_COUNT, LongType.instance)
                           .addRegularColumn(EXPIRED_BYTES, LongType.instance)
                           .addRegularColumn(ERROR_COUNT, LongType.instance)
                           .addRegularColumn(ERROR_BYTES, LongType.instance)
                           .addRegularColumn(OVERLOAD_COUNT, LongType.instance)
                           .addRegularColumn(OVERLOAD_BYTES, LongType.instance)
                           .addRegularColumn(ACTIVE_CONNECTION_COUNT, LongType.instance)
                           .addRegularColumn(CONNECTION_ATTEMPTS, LongType.instance)
                           .addRegularColumn(SUCCESSFUL_CONNECTION_ATTEMPTS, LongType.instance)
                           .build());
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        ByteBuffer[] addressAndPortBytes = ((CompositeType) metadata().partitionKeyType).split(partitionKey.getKey());
        InetAddress address = InetAddressType.instance.compose(addressAndPortBytes[0]);
        int port = Int32Type.instance.compose(addressAndPortBytes[1]);
        InetAddressAndPort addressAndPort = InetAddressAndPort.getByAddressOverrideDefaults(address, port);

        SimpleDataSet result = new SimpleDataSet(metadata());
        OutboundConnections connections = MessagingService.instance().channelManagers.get(addressAndPort);
        if (null != connections)
            addRow(result, addressAndPort, connections);
        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        MessagingService.instance()
                        .channelManagers
                        .forEach((addressAndPort, connections) -> addRow(result, addressAndPort, connections));
        return result;
    }

    private void addRow(SimpleDataSet dataSet, InetAddressAndPort addressAndPort, OutboundConnections connections)
    {
        String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(addressAndPort);
        String rack = DatabaseDescriptor.getEndpointSnitch().getRack(addressAndPort);
        long pendingBytes = sum(connections, OutboundConnection::pendingBytes);
        dataSet.row(addressAndPort.getAddress(), addressAndPort.getPort(), dc, rack)
               .column(USING_BYTES, pendingBytes)
               .column(USING_RESERVE_BYTES, connections.usingReserveBytes())
               .column(PENDING_COUNT, sum(connections, OutboundConnection::pendingCount))
               .column(PENDING_BYTES, pendingBytes)
               .column(SENT_COUNT, sum(connections, OutboundConnection::sentCount))
               .column(SENT_BYTES, sum(connections, OutboundConnection::sentBytes))
               .column(EXPIRED_COUNT, sum(connections, OutboundConnection::expiredCount))
               .column(EXPIRED_BYTES, sum(connections, OutboundConnection::expiredBytes))
               .column(ERROR_COUNT, sum(connections, OutboundConnection::errorCount))
               .column(ERROR_BYTES, sum(connections, OutboundConnection::errorBytes))
               .column(OVERLOAD_COUNT, sum(connections, OutboundConnection::overloadedCount))
               .column(OVERLOAD_BYTES, sum(connections, OutboundConnection::overloadedBytes))
               .column(ACTIVE_CONNECTION_COUNT, sum(connections, c -> c.isConnected() ? 1 : 0))
               .column(CONNECTION_ATTEMPTS, sum(connections, OutboundConnection::connectionAttempts))
               .column(SUCCESSFUL_CONNECTION_ATTEMPTS, sum(connections, OutboundConnection::successfulConnections));
    }

    private static long sum(OutboundConnections connections, ToLongFunction<OutboundConnection> f)
    {
        return f.applyAsLong(connections.small) + f.applyAsLong(connections.large) + f.applyAsLong(connections.urgent);
    }
}
