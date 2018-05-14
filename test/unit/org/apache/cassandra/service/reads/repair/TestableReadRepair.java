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

package org.apache.cassandra.service.reads.repair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.reads.DigestResolver;

public class TestableReadRepair implements ReadRepair
{
    public final Map<InetAddressAndPort, Mutation> sent = new HashMap<>();

    private final ReadCommand command;
    private final ConsistencyLevel consistency;

    public TestableReadRepair(ReadCommand command, ConsistencyLevel consistency)
    {
        this.command = command;
        this.consistency = consistency;
    }

    @Override
    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddressAndPort[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, consistency, this);
    }

    @Override
    public void startRepair(DigestResolver digestResolver, List<InetAddressAndPort> allEndpoints, List<InetAddressAndPort> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {

    }

    @Override
    public void awaitReads() throws ReadTimeoutException
    {

    }

    @Override
    public void maybeSendAdditionalReads()
    {

    }

    @Override
    public void maybeSendAdditionalWrites()
    {

    }

    @Override
    public void awaitWrites()
    {

    }

    @Override
    public void repairPartition(DecoratedKey key, Map<InetAddressAndPort, Mutation> mutations, InetAddressAndPort[] destinations)
    {
        sent.putAll(mutations);
    }

    public Mutation getForEndpoint(InetAddressAndPort endpoint)
    {
        return sent.get(endpoint);
    }
}
