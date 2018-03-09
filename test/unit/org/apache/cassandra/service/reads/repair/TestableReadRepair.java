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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.ResponseResolver;
import org.apache.cassandra.tracing.TraceState;

public class TestableReadRepair implements ReadRepair, RepairListener
{
    public final Map<InetAddressAndPort, Mutation> sent = new HashMap<>();

    private final ReadCommand command;

    public TestableReadRepair(ReadCommand command)
    {
        this.command = command;
    }

    private class TestablePartitionRepair implements RepairListener.PartitionRepair
    {
        @Override
        public void reportMutation(InetAddressAndPort endpoint, Mutation mutation)
        {
            sent.put(endpoint, mutation);
        }

        @Override
        public void finish()
        {

        }
    }

    @Override
    public UnfilteredPartitionIterators.MergeListener getMergeListener(InetAddressAndPort[] endpoints)
    {
        return new PartitionIteratorMergeListener(endpoints, command, this);
    }

    @Override
    public void startForegroundRepair(DigestResolver digestResolver, List<InetAddressAndPort> allEndpoints, List<InetAddressAndPort> contactedEndpoints, Consumer<PartitionIterator> resultConsumer)
    {

    }

    @Override
    public void awaitForegroundRepairFinish() throws ReadTimeoutException
    {

    }

    @Override
    public void maybeStartBackgroundRepair(ResponseResolver resolver)
    {

    }

    @Override
    public void backgroundDigestRepair(TraceState traceState)
    {

    }

    @Override
    public PartitionRepair startPartitionRepair()
    {
        return new TestablePartitionRepair();
    }

    @Override
    public void awaitRepairs(long timeoutMillis)
    {

    }

    public Mutation getForEndpoint(InetAddressAndPort endpoint)
    {
        return sent.get(endpoint);
    }
}
