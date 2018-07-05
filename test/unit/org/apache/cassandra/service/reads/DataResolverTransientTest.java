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

package org.apache.cassandra.service.reads;

import java.util.concurrent.TimeUnit;

import com.google.common.primitives.Ints;

import org.apache.cassandra.Util;
import org.apache.cassandra.db.DecoratedKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.EmptyIterators;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.SimpleBuilders;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.BTreeRow;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.ReplicaLayout;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.reads.repair.TestableReadRepair;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.db.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.locator.Replica.fullReplica;
import static org.apache.cassandra.locator.Replica.transientReplica;
import static org.apache.cassandra.locator.ReplicaUtils.full;
import static org.apache.cassandra.locator.ReplicaUtils.trans;

/**
 * Tests DataResolvers handing of transient replicas
 */
public class DataResolverTransientTest extends AbstractReadResponseTest
{
    private static DecoratedKey key;

    @Before
    public void setUp()
    {
        key = Util.dk("key1");
    }

    private static PartitionUpdate.Builder update(TableMetadata metadata, String key, Row... rows)
    {
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(metadata, dk(key), metadata.regularAndStaticColumns(), rows.length, false);
        for (Row row: rows)
        {
            builder.add(row);
        }
        return builder;
    }

    private static PartitionUpdate.Builder update(Row... rows)
    {
        return update(cfm, "key1", rows);
    }

    private static Row.SimpleBuilder rowBuilder(int clustering)
    {
        return new SimpleBuilders.RowBuilder(cfm, Integer.toString(clustering));
    }

    private static Row row(long timestamp, int clustering, int value)
    {
        return rowBuilder(clustering).timestamp(timestamp).add("c1", Integer.toString(value)).build();
    }

    private static DeletionTime deletion(long timeMillis)
    {
        TimeUnit MILLIS = TimeUnit.MILLISECONDS;
        return new DeletionTime(MILLIS.toMicros(timeMillis), Ints.checkedCast(MILLIS.toSeconds(timeMillis)));
    }

    /**
     * Tests that the given update doesn't cause data resolver to attempt to repair a transient replica
     */
    private void assertNoTransientRepairs(PartitionUpdate update)
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(update.metadata(), nowInSec, key);
        EndpointsForToken targetReplicas = EndpointsForToken.of(key.getToken(), full(EP1), full(EP2), trans(EP3));
        TestableReadRepair repair = new TestableReadRepair(command, QUORUM);
        DataResolver resolver = new DataResolver(command, plan(targetReplicas, ConsistencyLevel.QUORUM), repair, 0);

        Assert.assertFalse(resolver.isDataPresent());
        resolver.preprocess(response(command, EP1, iter(update), false));
        resolver.preprocess(response(command, EP2, iter(update), false));
        resolver.preprocess(response(command, EP3, EmptyIterators.unfilteredPartition(update.metadata()), false));

        Assert.assertFalse(repair.dataWasConsumed());
        assertPartitionsEqual(filter(iter(update)), resolver.resolve());
        Assert.assertTrue(repair.dataWasConsumed());
        Assert.assertTrue(repair.sent.toString(), repair.sent.isEmpty());
    }

    @Test
    public void emptyRowRepair()
    {
        assertNoTransientRepairs(update(row(1000, 4, 4), row(1000, 5, 5)).build());
    }

    @Test
    public void emptyPartitionDeletionRepairs()
    {
        PartitionUpdate.Builder builder = update();
        builder.addPartitionDeletion(deletion(1999));
        assertNoTransientRepairs(builder.build());
    }

    /**
     * Partition level deletion responses shouldn't sent data to a transient replica
     */
    @Test
    public void emptyRowDeletionRepairs()
    {
        PartitionUpdate.Builder builder = update();
        builder.add(rowBuilder(1).timestamp(1999).delete().build());
        assertNoTransientRepairs(builder.build());
    }

    @Test
    public void emptyComplexDeletionRepair()
    {

        long[] ts = {1000, 2000};

        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        builder.addComplexDeletion(m, new DeletionTime(ts[0] - 1, nowInSec));
        assertNoTransientRepairs(update(cfm2, "key", builder.build()).build());

    }

    @Test
    public void emptyRangeTombstoneRepairs()
    {
        Slice slice = Slice.make(Clustering.make(ByteBufferUtil.bytes("a")), Clustering.make(ByteBufferUtil.bytes("b")));
        PartitionUpdate.Builder builder = update();
        builder.add(new RangeTombstone(slice, deletion(2000)));
        assertNoTransientRepairs(builder.build());
    }

    /**
     * If the full replicas need to repair each other, repairs shouldn't be sent to transient replicas
     */
    @Test
    public void fullRepairsIgnoreTransientReplicas()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk(5));
        EndpointsForToken targetReplicas = EndpointsForToken.of(key.getToken(), full(EP1), full(EP2), trans(EP3));
        TestableReadRepair repair = new TestableReadRepair(command, QUORUM);
        DataResolver resolver = new DataResolver(command, plan(targetReplicas, QUORUM), repair, 0);

        Assert.assertFalse(resolver.isDataPresent());
        resolver.preprocess(response(command, EP1, iter(update(row(1000, 5, 5)).build()), false));
        resolver.preprocess(response(command, EP2, iter(update(row(2000, 4, 4)).build()), false));
        resolver.preprocess(response(command, EP3, EmptyIterators.unfilteredPartition(cfm), false));

        Assert.assertFalse(repair.dataWasConsumed());

        consume(resolver.resolve());

        Assert.assertTrue(repair.dataWasConsumed());

        Assert.assertTrue(repair.sent.containsKey(EP1));
        Assert.assertTrue(repair.sent.containsKey(EP2));
        Assert.assertFalse(repair.sent.containsKey(EP3));
    }

    /**
     * If the transient replica has new data, the full replicas shoould be repaired, the transient one should not
     */
    @Test
    public void transientMismatchesRepairFullReplicas()
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(cfm, nowInSec, dk(5));
        EndpointsForToken targetReplicas = EndpointsForToken.of(key.getToken(), full(EP1), full(EP2), trans(EP3));
        TestableReadRepair<?, ?> repair = new TestableReadRepair(command, QUORUM);
        DataResolver resolver = new DataResolver(command, plan(targetReplicas, QUORUM), repair, 0);

        Assert.assertFalse(resolver.isDataPresent());
        PartitionUpdate transData = update(row(1000, 5, 5)).build();
        resolver.preprocess(response(command, EP1, EmptyIterators.unfilteredPartition(cfm), false));
        resolver.preprocess(response(command, EP2, EmptyIterators.unfilteredPartition(cfm), false));
        resolver.preprocess(response(command, EP3, iter(transData), false));

        Assert.assertFalse(repair.dataWasConsumed());

        assertPartitionsEqual(filter(iter(transData)), resolver.resolve());

        Assert.assertTrue(repair.dataWasConsumed());

        assertPartitionsEqual(filter(iter(transData)), filter(iter(repair.sent.get(EP1).getPartitionUpdate(cfm))));
        assertPartitionsEqual(filter(iter(transData)), filter(iter(repair.sent.get(EP2).getPartitionUpdate(cfm))));
        Assert.assertFalse(repair.sent.containsKey(EP3));

    }

    private ReplicaLayout.ForToken plan(EndpointsForToken replicas, ConsistencyLevel consistencyLevel)
    {
        return new ReplicaLayout.ForToken(ks, consistencyLevel, replicas.token(), replicas, null, replicas);
    }
}
