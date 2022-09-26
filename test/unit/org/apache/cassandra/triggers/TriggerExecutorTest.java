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
package org.apache.cassandra.triggers;

import java.util.*;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TriggerExecutorTest
{
    @BeforeClass
    public static void setupDD()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void sameKeySameCfColumnFamilies() throws ConfigurationException, InvalidRequestException
    {
        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", SameKeySameCfTrigger.class.getName()));
        // origin column 'c1' = "v1", augment extra column 'c2' = "trigger"
        PartitionUpdate mutated = TriggerExecutor.instance.execute(makeCf(metadata, "k1", "v1", null));

        List<Row> rows = new ArrayList<>();
        try (RowIterator iterator = UnfilteredRowIterators.filter(mutated.unfilteredIterator(),
                                                                  FBUtilities.nowInSeconds()))
        {
            iterator.forEachRemaining(rows::add);
        }

        // only 1 row
        assertEquals(1, rows.size());

        List<Cell<?>> cells = new ArrayList<>();
        rows.get(0).cells().forEach(cells::add);

        // 2 columns
        assertEquals(2, cells.size());

        // check column 'c1'
        assertEquals(bytes("v1"), cells.get(0).value());
        // check column 'c2'
        assertEquals(bytes("trigger"), cells.get(1).value());
    }

    @Test(expected = InvalidRequestException.class)
    public void sameKeyDifferentCfColumnFamilies() throws ConfigurationException, InvalidRequestException
    {
        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", SameKeyDifferentCfTrigger.class.getName()));
        TriggerExecutor.instance.execute(makeCf(metadata, "k1", "v1", null));
    }

    @Test(expected = InvalidRequestException.class)
    public void differentKeyColumnFamilies() throws ConfigurationException, InvalidRequestException
    {
        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", DifferentKeyTrigger.class.getName()));
        TriggerExecutor.instance.execute(makeCf(metadata, "k1", "v1", null));
    }

    @Test
    public void noTriggerMutations() throws ConfigurationException, InvalidRequestException
    {
        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", NoOpTrigger.class.getName()));
        Mutation rm = new Mutation(makeCf(metadata, "k1", "v1", null));
        assertNull(TriggerExecutor.instance.execute(Collections.singletonList(rm)));
    }

    @Test
    public void sameKeySameCfRowMutations() throws ConfigurationException, InvalidRequestException
    {
        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", SameKeySameCfTrigger.class.getName()));
        PartitionUpdate cf1 = makeCf(metadata, "k1", "k1v1", null);
        PartitionUpdate cf2 = makeCf(metadata, "k2", "k2v1", null);
        Mutation rm1 = new Mutation.PartitionUpdateCollector("ks1", cf1.partitionKey()).add(cf1).build();
        Mutation rm2 = new Mutation.PartitionUpdateCollector("ks1", cf2.partitionKey()).add(cf2).build();

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm1, rm2)));
        assertEquals(2, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        List<PartitionUpdate> mutatedCFs = new ArrayList<>(tmutations.get(0).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        Row row = mutatedCFs.get(0).iterator().next();
        assertEquals(bytes("k1v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
        assertEquals(bytes("trigger"), row.getCell(metadata.getColumn(bytes("c2"))).value());

        mutatedCFs = new ArrayList<>(tmutations.get(1).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        row = mutatedCFs.get(0).iterator().next();
        assertEquals(bytes("k2v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
        assertEquals(bytes("trigger"), row.getCell(metadata.getColumn(bytes("c2"))).value());
    }

    @Test
    public void sameKeySameCfPartialRowMutations() throws ConfigurationException, InvalidRequestException
    {
        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", SameKeySameCfPartialTrigger.class.getName()));
        PartitionUpdate cf1 = makeCf(metadata, "k1", "k1v1", null);
        PartitionUpdate cf2 = makeCf(metadata, "k2", "k2v1", null);
        Mutation rm1 = new Mutation.PartitionUpdateCollector("ks1", cf1.partitionKey()).add(cf1).build();
        Mutation rm2 = new Mutation.PartitionUpdateCollector("ks1", cf2.partitionKey()).add(cf2).build();

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm1, rm2)));
        assertEquals(2, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        List<PartitionUpdate> mutatedCFs = new ArrayList<>(tmutations.get(0).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        Row row = mutatedCFs.get(0).iterator().next();
        assertEquals(bytes("k1v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
        assertNull(row.getCell(metadata.getColumn(bytes("c2"))));

        mutatedCFs = new ArrayList<>(tmutations.get(1).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        row = mutatedCFs.get(0).iterator().next();
        assertEquals(bytes("k2v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
        assertEquals(bytes("trigger"), row.getCell(metadata.getColumn(bytes("c2"))).value());
    }

    @Test
    public void sameKeyDifferentCfRowMutations() throws ConfigurationException, InvalidRequestException
    {
        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", SameKeyDifferentCfTrigger.class.getName()));
        PartitionUpdate cf1 = makeCf(metadata, "k1", "k1v1", null);
        PartitionUpdate cf2 = makeCf(metadata, "k2", "k2v1", null);
        Mutation rm1 = new Mutation.PartitionUpdateCollector("ks1", cf1.partitionKey()).add(cf1).build();
        Mutation rm2 = new Mutation.PartitionUpdateCollector("ks1", cf2.partitionKey()).add(cf2).build();

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm1, rm2)));
        assertEquals(2, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        List<PartitionUpdate> mutatedCFs = new ArrayList<>(tmutations.get(0).getPartitionUpdates());
        assertEquals(2, mutatedCFs.size());
        for (PartitionUpdate update : mutatedCFs)
        {
            if (update.metadata().name.equals("cf1"))
            {
                Row row = update.iterator().next();
                assertEquals(bytes("k1v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
                assertNull(row.getCell(metadata.getColumn(bytes("c2"))));
            }
            else
            {
                Row row = update.iterator().next();
                assertNull(row.getCell(metadata.getColumn(bytes("c1"))));
                assertEquals(bytes("trigger"), row.getCell(metadata.getColumn(bytes("c2"))).value());
            }
        }

        mutatedCFs = new ArrayList<>(tmutations.get(1).getPartitionUpdates());
        assertEquals(2, mutatedCFs.size());

        for (PartitionUpdate update : mutatedCFs)
        {
            if (update.metadata().name.equals("cf1"))
            {
                Row row = update.iterator().next();
                assertEquals(bytes("k2v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
                assertNull(row.getCell(metadata.getColumn(bytes("c2"))));
            }
            else
            {
                Row row = update.iterator().next();
                assertNull(row.getCell(metadata.getColumn(bytes("c1"))));
                assertEquals(bytes("trigger"), row.getCell(metadata.getColumn(bytes("c2"))).value());
            }
        }
    }

    @Test
    public void sameKeyDifferentKsRowMutations() throws ConfigurationException, InvalidRequestException
    {
        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", SameKeyDifferentKsTrigger.class.getName()));
        PartitionUpdate cf1 = makeCf(metadata, "k1", "k1v1", null);
        PartitionUpdate cf2 = makeCf(metadata, "k2", "k2v1", null);
        Mutation rm1 = new Mutation.PartitionUpdateCollector("ks1", cf1.partitionKey()).add(cf1).build();
        Mutation rm2 = new Mutation.PartitionUpdateCollector("ks1", cf2.partitionKey()).add(cf2).build();

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm1, rm2)));
        assertEquals(4, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        List<PartitionUpdate> mutatedCFs = new ArrayList<>(tmutations.get(0).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        Row row = mutatedCFs.get(0).iterator().next();
        assertEquals(bytes("k1v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
        assertNull(row.getCell(metadata.getColumn(bytes("c2"))));

        mutatedCFs = new ArrayList<>(tmutations.get(1).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        row = mutatedCFs.get(0).iterator().next();
        assertEquals(bytes("k2v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
        assertNull(row.getCell(metadata.getColumn(bytes("c2"))));

        mutatedCFs = new ArrayList<>(tmutations.get(2).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        row = mutatedCFs.get(0).iterator().next();
        assertNull(row.getCell(metadata.getColumn(bytes("c1"))));
        assertEquals(bytes("trigger"), row.getCell(metadata.getColumn(bytes("c2"))).value());

        mutatedCFs = new ArrayList<>(tmutations.get(3).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        row = mutatedCFs.get(0).iterator().next();
        assertNull(row.getCell(metadata.getColumn(bytes("c1"))));
        assertEquals(bytes("trigger"), row.getCell(metadata.getColumn(bytes("c2"))).value());
    }

    @Test
    public void differentKeyRowMutations() throws ConfigurationException, InvalidRequestException
    {

        TableMetadata metadata = makeTableMetadata("ks1", "cf1", TriggerMetadata.create("test", DifferentKeyTrigger.class.getName()));
        PartitionUpdate cf1 = makeCf(metadata, "k1", "v1", null);
        Mutation rm = new Mutation.PartitionUpdateCollector("ks1", cf1.partitionKey()).add(cf1).build();

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm)));
        assertEquals(2, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        assertEquals(bytes("k1"), tmutations.get(0).key().getKey());
        assertEquals(bytes("otherKey"), tmutations.get(1).key().getKey());

        List<PartitionUpdate> mutatedCFs = new ArrayList<>(tmutations.get(0).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        Row row = mutatedCFs.get(0).iterator().next();
        assertEquals(bytes("v1"), row.getCell(metadata.getColumn(bytes("c1"))).value());
        assertNull(row.getCell(metadata.getColumn(bytes("c2"))));

        mutatedCFs = new ArrayList<>(tmutations.get(1).getPartitionUpdates());
        assertEquals(1, mutatedCFs.size());
        row = mutatedCFs.get(0).iterator().next();
        assertEquals(bytes("trigger"), row.getCell(metadata.getColumn(bytes("c2"))).value());
        assertNull(row.getCell(metadata.getColumn(bytes("c1"))));
    }

    private static TableMetadata makeTableMetadata(String ks, String cf, TriggerMetadata trigger)
    {
        TableMetadata.Builder builder =
            TableMetadata.builder(ks, cf)
                         .addPartitionKeyColumn("pkey", UTF8Type.instance)
                         .addRegularColumn("c1", UTF8Type.instance)
                         .addRegularColumn("c2", UTF8Type.instance);

        if (trigger != null)
            builder.triggers(Triggers.of(trigger));

        return builder.build();
    }

    private static PartitionUpdate makeCf(TableMetadata metadata, String key, String columnValue1, String columnValue2)
    {
        Row.Builder builder = BTreeRow.unsortedBuilder();
        builder.newRow(Clustering.EMPTY);
        long ts = FBUtilities.timestampMicros();
        if (columnValue1 != null)
            builder.addCell(BufferCell.live(metadata.getColumn(bytes("c1")), ts, bytes(columnValue1)));
        if (columnValue2 != null)
            builder.addCell(BufferCell.live(metadata.getColumn(bytes("c2")), ts, bytes(columnValue2)));

        return PartitionUpdate.singleRowUpdate(metadata, Util.dk(key), builder.build());
    }

    public static class NoOpTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            return null;
        }
    }

    public static class SameKeySameCfTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(partition.metadata(), FBUtilities.timestampMicros(), partition.partitionKey().getKey());
            builder.add("c2", bytes("trigger"));
            return Collections.singletonList(builder.build());
        }
    }

    public static class SameKeySameCfPartialTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            if (!partition.partitionKey().getKey().equals(bytes("k2")))
                return null;

            RowUpdateBuilder builder = new RowUpdateBuilder(partition.metadata(), FBUtilities.timestampMicros(), partition.partitionKey().getKey());
            builder.add("c2", bytes("trigger"));
            return Collections.singletonList(builder.build());
        }
    }

    public static class SameKeyDifferentCfTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(makeTableMetadata(partition.metadata().keyspace, "otherCf", null), FBUtilities.timestampMicros(), partition.partitionKey().getKey());
            builder.add("c2", bytes("trigger"));
            return Collections.singletonList(builder.build());
        }
    }

    public static class SameKeyDifferentKsTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(makeTableMetadata("otherKs", "otherCf", null), FBUtilities.timestampMicros(), partition.partitionKey().getKey());
            builder.add("c2", bytes("trigger"));
            return Collections.singletonList(builder.build());
        }
    }

    public static class DifferentKeyTrigger implements ITrigger
    {
        public Collection<Mutation> augment(Partition partition)
        {
            RowUpdateBuilder builder = new RowUpdateBuilder(makeTableMetadata("otherKs", "otherCf", null), FBUtilities.timestampMicros(), "otherKey");
            builder.add("c2", bytes("trigger"));
            return Collections.singletonList(builder.build());
        }
    }

    private static class RmComparator implements Comparator<IMutation>
    {
        public int compare(IMutation m1, IMutation m2)
        {
            int cmp = m1.getKeyspaceName().compareTo(m2.getKeyspaceName());
            return cmp != 0 ? cmp : m1.key().compareTo(m2.key());
        }
    }
}
