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
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.FailureDetectorMBean;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.hints.PendingHintsInfo;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

final class PendingHintsTable extends AbstractVirtualTable
{
    private static final String HOST_ID = "host_id";
    private static final String ADDRESS = "address";
    private static final String PORT = "port";
    private static final String RACK = "rack";
    private static final String DC = "dc";
    private static final String STATUS = "status";
    private static final String FILES = "files";
    private static final String NEWEST = "newest";
    private static final String OLDEST = "oldest";

    PendingHintsTable(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "pending_hints")
                           .comment("Pending hints that this node has for other nodes")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .partitioner(new LocalPartitioner(UUIDType.instance))
                           .addPartitionKeyColumn(HOST_ID, UUIDType.instance)
                           .addRegularColumn(ADDRESS, InetAddressType.instance)
                           .addRegularColumn(PORT, Int32Type.instance)
                           .addRegularColumn(RACK, UTF8Type.instance)
                           .addRegularColumn(DC, UTF8Type.instance)
                           .addRegularColumn(STATUS, UTF8Type.instance)
                           .addRegularColumn(FILES, Int32Type.instance)
                           .addRegularColumn(NEWEST, TimestampType.instance)
                           .addRegularColumn(OLDEST, TimestampType.instance)
                           .build());
    }

    @Override
    public DataSet data()
    {
        List<PendingHintsInfo> pendingHints = HintsService.instance.getPendingHintsInfo();
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();

        SimpleDataSet result = new SimpleDataSet(metadata());

        Map<String, String> simpleStates;
        if (FailureDetector.instance instanceof FailureDetectorMBean)
            simpleStates = ((FailureDetectorMBean) FailureDetector.instance).getSimpleStatesWithPort();
        else
            simpleStates = Collections.emptyMap();

        for (PendingHintsInfo info : pendingHints)
        {
            InetAddressAndPort addressAndPort = StorageService.instance.getEndpointForHostId(info.hostId);
            InetAddress address = null;
            Integer port = null;
            String rack = "Unknown";
            String dc = "Unknown";
            String status = "Unknown";
            if (addressAndPort != null)
            {
                address = addressAndPort.getAddress();
                port = addressAndPort.getPort();
                rack = snitch.getRack(addressAndPort);
                dc = snitch.getDatacenter(addressAndPort);
                status = simpleStates.getOrDefault(addressAndPort.toString(), status);
            }
            result.row(info.hostId)
                  .column(ADDRESS, address)
                  .column(PORT, port)
                  .column(RACK, rack)
                  .column(DC, dc)
                  .column(STATUS, status)
                  .column(FILES, info.totalFiles)
                  .column(NEWEST, new Date(info.newestTimestamp))
                  .column(OLDEST, new Date(info.oldestTimestamp));
        }
        return result;
    }
}
