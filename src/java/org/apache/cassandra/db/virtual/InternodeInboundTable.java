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
import org.apache.cassandra.net.InboundMessageHandlers;
import org.apache.cassandra.schema.TableMetadata;

public final class InternodeInboundTable extends AbstractVirtualTable
{
    private static final String ADDRESS = "address";
    private static final String PORT = "port";
    private static final String DC = "dc";
    private static final String RACK = "rack";

    private static final String USING_BYTES = "using_bytes";
    private static final String USING_RESERVE_BYTES = "using_reserve_bytes";
    private static final String CORRUPT_FRAMES_RECOVERED = "corrupt_frames_recovered";
    private static final String CORRUPT_FRAMES_UNRECOVERED = "corrupt_frames_unrecovered";
    private static final String ERROR_BYTES = "error_bytes";
    private static final String ERROR_COUNT = "error_count";
    private static final String EXPIRED_BYTES = "expired_bytes";
    private static final String EXPIRED_COUNT = "expired_count";
    private static final String SCHEDULED_BYTES = "scheduled_bytes";
    private static final String SCHEDULED_COUNT = "scheduled_count";
    private static final String PROCESSED_BYTES = "processed_bytes";
    private static final String PROCESSED_COUNT = "processed_count";
    private static final String RECEIVED_BYTES = "received_bytes";
    private static final String RECEIVED_COUNT = "received_count";
    private static final String THROTTLED_COUNT = "throttled_count";
    private static final String THROTTLED_NANOS = "throttled_nanos";

    InternodeInboundTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "internode_inbound")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(CompositeType.getInstance(InetAddressType.instance, Int32Type.instance)))
                           .addPartitionKeyColumn(ADDRESS, InetAddressType.instance)
                           .addPartitionKeyColumn(PORT, Int32Type.instance)
                           .addClusteringColumn(DC, UTF8Type.instance)
                           .addClusteringColumn(RACK, UTF8Type.instance)
                           .addRegularColumn(USING_BYTES, LongType.instance)
                           .addRegularColumn(USING_RESERVE_BYTES, LongType.instance)
                           .addRegularColumn(CORRUPT_FRAMES_RECOVERED, LongType.instance)
                           .addRegularColumn(CORRUPT_FRAMES_UNRECOVERED, LongType.instance)
                           .addRegularColumn(ERROR_BYTES, LongType.instance)
                           .addRegularColumn(ERROR_COUNT, LongType.instance)
                           .addRegularColumn(EXPIRED_BYTES, LongType.instance)
                           .addRegularColumn(EXPIRED_COUNT, LongType.instance)
                           .addRegularColumn(SCHEDULED_BYTES, LongType.instance)
                           .addRegularColumn(SCHEDULED_COUNT, LongType.instance)
                           .addRegularColumn(PROCESSED_BYTES, LongType.instance)
                           .addRegularColumn(PROCESSED_COUNT, LongType.instance)
                           .addRegularColumn(RECEIVED_BYTES, LongType.instance)
                           .addRegularColumn(RECEIVED_COUNT, LongType.instance)
                           .addRegularColumn(THROTTLED_COUNT, LongType.instance)
                           .addRegularColumn(THROTTLED_NANOS, LongType.instance)
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
        InboundMessageHandlers handlers = MessagingService.instance().messageHandlers.get(addressAndPort);
        if (null != handlers)
            addRow(result, addressAndPort, handlers);
        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        MessagingService.instance()
                        .messageHandlers
                        .forEach((addressAndPort, handlers) -> addRow(result, addressAndPort, handlers));
        return result;
    }

    private void addRow(SimpleDataSet dataSet, InetAddressAndPort addressAndPort, InboundMessageHandlers handlers)
    {
        String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(addressAndPort);
        String rack = DatabaseDescriptor.getEndpointSnitch().getRack(addressAndPort);
        dataSet.row(addressAndPort.getAddress(), addressAndPort.getPort(), dc, rack)
               .column(USING_BYTES, handlers.usingCapacity())
               .column(USING_RESERVE_BYTES, handlers.usingEndpointReserveCapacity())
               .column(CORRUPT_FRAMES_RECOVERED, handlers.corruptFramesRecovered())
               .column(CORRUPT_FRAMES_UNRECOVERED, handlers.corruptFramesUnrecovered())
               .column(ERROR_BYTES, handlers.errorBytes())
               .column(ERROR_COUNT, handlers.errorCount())
               .column(EXPIRED_BYTES, handlers.expiredBytes())
               .column(EXPIRED_COUNT, handlers.expiredCount())
               .column(SCHEDULED_BYTES, handlers.scheduledBytes())
               .column(SCHEDULED_COUNT, handlers.scheduledCount())
               .column(PROCESSED_BYTES, handlers.processedBytes())
               .column(PROCESSED_COUNT, handlers.processedCount())
               .column(RECEIVED_BYTES, handlers.receivedBytes())
               .column(RECEIVED_COUNT, handlers.receivedCount())
               .column(THROTTLED_COUNT, handlers.throttledCount())
               .column(THROTTLED_NANOS, handlers.throttledNanos());
    }
}
