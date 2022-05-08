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

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.service.reads.DigestResolver;

public class TestableReadRepair<E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>>
        implements ReadRepair<E, P>
{
    public final Map<InetAddressAndPort, Mutation> sent = new HashMap<>();

    private final ReadCommand command;

    private boolean partitionListenerClosed = false;
    private boolean rowListenerClosed = true;

    public TestableReadRepair(ReadCommand command)
    {
        this.command = command;
    }

    @Override
    public UnfilteredPartitionIterators.MergeListener getMergeListener(P endpoints)
    {
        return new PartitionIteratorMergeListener<E>(endpoints, command, this) {
            @Override
            public void close()
            {
                super.close();
                partitionListenerClosed = true;
            }

            @Override
            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                assert rowListenerClosed;
                rowListenerClosed = false;
                return new RowIteratorMergeListener<E>(partitionKey, columns(versions), isReversed(versions), endpoints, command, TestableReadRepair.this) {
                    @Override
                    public void close()
                    {
                        super.close();
                        rowListenerClosed = true;
                    }
                };
            }
        };
    }

    @Override
    public void startRepair(DigestResolver<E, P> digestResolver, Consumer<PartitionIterator> resultConsumer)
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
    public void repairPartition(DecoratedKey partitionKey, Map<Replica, Mutation> mutations, ReplicaPlan.ForWrite writePlan)
    {
        for (Map.Entry<Replica, Mutation> entry: mutations.entrySet())
            sent.put(entry.getKey().endpoint(), entry.getValue());
    }

    public Mutation getForEndpoint(InetAddressAndPort endpoint)
    {
        return sent.get(endpoint);
    }

    public boolean dataWasConsumed()
    {
        return partitionListenerClosed && rowListenerClosed;
    }
}
