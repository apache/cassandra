package org.apache.cassandra.service;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.Schema;
import org.junit.Test;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.RandomPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConsistencyLevelTest extends CleanupHelper
{
    @Test
    public void testReadWriteConsistencyChecks() throws Exception
    {
        StorageService ss = StorageService.instance;
        final int RING_SIZE = 3;

        TokenMetadata tmd = ss.getTokenMetadata();
        tmd.clearUnsafe();
        IPartitioner partitioner = new RandomPartitioner();

        ss.setPartitionerUnsafe(partitioner);

        ArrayList<Token> endpointTokens = new ArrayList<Token>();
        ArrayList<Token> keyTokens = new ArrayList<Token>();
        List<InetAddress> hostsInUse = new ArrayList<InetAddress>();
        List<InetAddress> hosts = new ArrayList<InetAddress>();

        Util.createInitialRing(ss, partitioner, endpointTokens, keyTokens, hosts, RING_SIZE);

        AbstractReplicationStrategy strategy;

        for (final String table : Schema.instance.getNonSystemTables())
        {
            strategy = getStrategy(table, tmd);
            StorageService.calculatePendingRanges(strategy, table);
            int replicationFactor = strategy.getReplicationFactor();
            if (replicationFactor < 2)
                continue;

            for (ConsistencyLevel c : ConsistencyLevel.values())
            {

                if (c == ConsistencyLevel.EACH_QUORUM || c == ConsistencyLevel.LOCAL_QUORUM)
                    continue;

                for (int i = 0; i < replicationFactor; i++)
                {
                    hostsInUse.clear();
                    for (int j = 0 ; j < i ; j++)
                    {
                        hostsInUse.add(hosts.get(j));
                    }

                    if (hostsInUse.isEmpty())
                    {
                        // We skip this case as it means RF = 0 in this simulation.
                        continue;
                    }

                    IWriteResponseHandler writeHandler = strategy.getWriteResponseHandler(hostsInUse, c);

                    IReadCommand command = new IReadCommand()
                    {
                        public String getKeyspace()
                        {
                            return table;
                        }
                    };
                    RowRepairResolver resolver = new RowRepairResolver(table, ByteBufferUtil.bytes("foo"));
                    ReadCallback<Row> readHandler = StorageProxy.getReadCallback(resolver, command, c, hostsInUse);

                    boolean isWriteUnavailable = false;
                    boolean isReadUnavailable = false;
                    try
                    {
                        writeHandler.assureSufficientLiveNodes();
                    }
                    catch (UnavailableException e)
                    {
                        isWriteUnavailable = true;
                    }

                    try
                    {
                        readHandler.assureSufficientLiveNodes();
                    }
                    catch (UnavailableException e)
                    {
                        isReadUnavailable = true;
                    }

                    //these should always match (in this kind of test)
                    assertTrue(String.format("Node Alive: %d - CL: %s - isWriteUnavailable: %b - isReadUnavailable: %b", hostsInUse.size(), c, isWriteUnavailable, isReadUnavailable),
                               isWriteUnavailable == isReadUnavailable);

                    switch (c)
                    {
                        case ALL:
                            if (isWriteUnavailable)
                                assertTrue(hostsInUse.size() < replicationFactor);
                            else
                                assertTrue(hostsInUse.size() >= replicationFactor);

                            break;
                        case ONE:
                        case ANY:
                            if (isWriteUnavailable)
                                assertTrue(hostsInUse.size() == 0);
                            else
                                assertTrue(hostsInUse.size() > 0);
                            break;
                        case TWO:
                            if (isWriteUnavailable)
                                assertTrue(hostsInUse.size() < 2);
                            else
                                assertTrue(hostsInUse.size() >= 2);
                            break;
                        case THREE:
                            if (isWriteUnavailable)
                                assertTrue(hostsInUse.size() < 3);
                            else
                                assertTrue(hostsInUse.size() >= 3);
                            break;
                        case QUORUM:
                            if (isWriteUnavailable)
                                assertTrue(hostsInUse.size() < (replicationFactor / 2 + 1));
                            else
                                assertTrue(hostsInUse.size() >= (replicationFactor / 2 + 1));
                            break;
                        default:
                            fail("Unhandled CL: " + c);

                    }
                }
            }
            return;
        }

        fail("Test requires at least one table with RF > 1");
    }

    private AbstractReplicationStrategy getStrategy(String table, TokenMetadata tmd) throws ConfigurationException
    {
        KSMetaData ksmd = Schema.instance.getKSMetaData(table);
        return AbstractReplicationStrategy.createReplicationStrategy(
                table,
                ksmd.strategyClass,
                tmd,
                new SimpleSnitch(),
                ksmd.strategyOptions);
    }

}
