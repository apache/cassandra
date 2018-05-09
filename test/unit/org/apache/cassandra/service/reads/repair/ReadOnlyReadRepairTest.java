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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.locator.Endpoints;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.service.reads.ReadCallback;

public class ReadOnlyReadRepairTest extends AbstractReadRepairTest
{
    private static class InstrumentedReadOnlyReadRepair<E extends Endpoints<E>, L extends ReplicaLayout<E, L>> extends ReadOnlyReadRepair implements InstrumentedReadRepair
    {
        public InstrumentedReadOnlyReadRepair(ReadCommand command, L replicaLayout, long queryStartNanoTime)
        {
            super(command, replicaLayout, queryStartNanoTime);
        }

        Set<InetAddressAndPort> readCommandRecipients = new HashSet<>();
        ReadCallback readCallback = null;

        @Override
        void sendReadCommand(Replica to, ReadCallback callback)
        {
            assert readCallback == null || readCallback == callback;
            readCommandRecipients.add(to.endpoint());
            readCallback = callback;
        }

        @Override
        public Set<InetAddressAndPort> getReadRecipients()
        {
            return readCommandRecipients;
        }

        @Override
        public ReadCallback getReadCallback()
        {
            return readCallback;
        }
    }

    @BeforeClass
    public static void setUpClass() throws Throwable
    {
        configureClass(ReadRepairStrategy.NONE);
    }

    @Override
    public InstrumentedReadRepair createInstrumentedReadRepair(ReadCommand command, ReplicaLayout<?, ?> replicaLayout, long queryStartNanoTime)
    {
        return new InstrumentedReadOnlyReadRepair(command, replicaLayout, queryStartNanoTime);
    }

    @Test
    public void getMergeListener()
    {
        ReplicaLayout<?, ?> replicaLayout = replicaLayout(replicas, replicas);
        InstrumentedReadRepair repair = createInstrumentedReadRepair(replicaLayout);
        Assert.assertSame(UnfilteredPartitionIterators.MergeListener.NOOP, repair.getMergeListener(replicaLayout));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void repairPartitionFailure()
    {
        ReplicaLayout<?, ?> replicaLayout = replicaLayout(replicas, replicas);
        InstrumentedReadRepair repair = createInstrumentedReadRepair(replicaLayout);
        repair.repairPartition(null, Collections.emptyMap(), replicaLayout);
    }
}
