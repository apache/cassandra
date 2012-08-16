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
package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.protocol.TMessage;

/**
 * Datacenter Quorum response handler blocks for a quorum of responses from the local DC
 */
public class DatacenterReadCallback<TMessage, TResolved> extends ReadCallback<TMessage, TResolved>
{
    private static final Comparator<InetAddress> localComparator = new Comparator<InetAddress>()
    {
        public int compare(InetAddress endpoint1, InetAddress endpoint2)
        {
            boolean local1 = localdc.equals(snitch.getDatacenter(endpoint1));
            boolean local2 = localdc.equals(snitch.getDatacenter(endpoint2));
            if (local1 && !local2)
                return -1;
            if (local2 && !local1)
                return 1;
            return 0;
        }
    };

    public DatacenterReadCallback(IResponseResolver resolver, ConsistencyLevel consistencyLevel, IReadCommand command, List<InetAddress> endpoints)
    {
        super(resolver, consistencyLevel, command, endpoints);
    }

    @Override
    protected void sortForConsistencyLevel(List<InetAddress> endpoints)
    {
        Collections.sort(endpoints, localComparator);
    }

    @Override
    protected boolean waitingFor(MessageIn message)
    {
        return localdc.equals(snitch.getDatacenter(message.from));
    }

    @Override
    public int determineBlockFor(ConsistencyLevel consistency_level, String table)
    {
        NetworkTopologyStrategy stategy = (NetworkTopologyStrategy) Table.open(table).getReplicationStrategy();
        return (stategy.getReplicationFactor(localdc) / 2) + 1;
    }

    @Override
    public void assureSufficientLiveNodes() throws UnavailableException
    {
        int localEndpoints = 0;
        for (InetAddress endpoint : endpoints)
        {
            if (localdc.equals(snitch.getDatacenter(endpoint)))
                localEndpoints++;
        }

        if (localEndpoints < blockfor)
        {
            if (logger.isDebugEnabled())
            {
                StringBuilder builder = new StringBuilder("Local replicas [");
                for (InetAddress endpoint : endpoints)
                {
                    if (localdc.equals(snitch.getDatacenter(endpoint)))
                        builder.append(endpoint).append(",");
                }
                builder.append("] are insufficient to satisfy LOCAL_QUORUM requirement of ").append(blockfor).append(" live nodes in '").append(localdc).append("'");
                logger.debug(builder.toString());
            }

            throw new UnavailableException();
        }
    }
}
