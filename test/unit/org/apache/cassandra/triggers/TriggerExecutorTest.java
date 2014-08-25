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

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.Test;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.TriggerDefinition;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;

import static org.junit.Assert.*;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class TriggerExecutorTest
{
    @Test
    public void sameKeySameCfColumnFamilies() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", SameKeySameCfTrigger.class.getName()));
        ColumnFamily mutated = TriggerExecutor.instance.execute(bytes("k1"), makeCf(metadata, "v1", null));
        assertEquals(bytes("v1"), mutated.getColumn(getColumnName(metadata, "c1")).value());
        assertEquals(bytes("trigger"), mutated.getColumn(getColumnName(metadata, "c2")).value());
    }

    @Test(expected = InvalidRequestException.class)
    public void sameKeyDifferentCfColumnFamilies() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", SameKeyDifferentCfTrigger.class.getName()));
        TriggerExecutor.instance.execute(bytes("k1"), makeCf(metadata, "v1", null));
    }

    @Test(expected = InvalidRequestException.class)
    public void differentKeyColumnFamilies() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", DifferentKeyTrigger.class.getName()));
        TriggerExecutor.instance.execute(bytes("k1"), makeCf(metadata, "v1", null));
    }

    @Test
    public void noTriggerMutations() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", NoOpTrigger.class.getName()));
        Mutation rm = new Mutation(bytes("k1"), makeCf(metadata, "v1", null));
        assertNull(TriggerExecutor.instance.execute(Collections.singletonList(rm)));
    }

    @Test
    public void sameKeySameCfRowMutations() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", SameKeySameCfTrigger.class.getName()));
        ColumnFamily cf1 = makeCf(metadata, "k1v1", null);
        ColumnFamily cf2 = makeCf(metadata, "k2v1", null);
        Mutation rm1 = new Mutation(bytes("k1"), cf1);
        Mutation rm2 = new Mutation(bytes("k2"), cf2);

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm1, rm2)));
        assertEquals(2, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        List<ColumnFamily> mutatedCFs = new ArrayList<>(tmutations.get(0).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertEquals(bytes("k1v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertEquals(bytes("trigger"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")).value());

        mutatedCFs = new ArrayList<>(tmutations.get(1).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertEquals(bytes("k2v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertEquals(bytes("trigger"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")).value());
    }

    @Test
    public void sameKeySameCfPartialRowMutations() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", SameKeySameCfPartialTrigger.class.getName()));
        ColumnFamily cf1 = makeCf(metadata, "k1v1", null);
        ColumnFamily cf2 = makeCf(metadata, "k2v1", null);
        Mutation rm1 = new Mutation(bytes("k1"), cf1);
        Mutation rm2 = new Mutation(bytes("k2"), cf2);

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm1, rm2)));
        assertEquals(2, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        List<ColumnFamily> mutatedCFs = new ArrayList<>(tmutations.get(0).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertEquals(bytes("k1v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")));

        mutatedCFs = new ArrayList<>(tmutations.get(1).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertEquals(bytes("k2v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertEquals(bytes("trigger"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")).value());
    }

    @Test
    public void sameKeyDifferentCfRowMutations() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", SameKeyDifferentCfTrigger.class.getName()));
        ColumnFamily cf1 = makeCf(metadata, "k1v1", null);
        ColumnFamily cf2 = makeCf(metadata, "k2v1", null);
        Mutation rm1 = new Mutation(bytes("k1"), cf1);
        Mutation rm2 = new Mutation(bytes("k2"), cf2);

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm1, rm2)));
        assertEquals(2, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        List<ColumnFamily> mutatedCFs = new ArrayList<>(tmutations.get(0).getColumnFamilies());
        assertEquals(2, mutatedCFs.size());

        Collections.sort(mutatedCFs, new CfComparator());
        assertEquals(bytes("k1v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")));
        assertNull(mutatedCFs.get(1).getColumn(getColumnName(metadata, "c1")));
        assertEquals(bytes("trigger"), mutatedCFs.get(1).getColumn(getColumnName(metadata, "c2")).value());

        mutatedCFs = new ArrayList<>(tmutations.get(1).getColumnFamilies());
        assertEquals(2, mutatedCFs.size());

        Collections.sort(mutatedCFs, new CfComparator());
        assertEquals(bytes("k2v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")));
        assertNull(mutatedCFs.get(1).getColumn(getColumnName(metadata, "c1")));
        assertEquals(bytes("trigger"), mutatedCFs.get(1).getColumn(getColumnName(metadata, "c2")).value());
    }

    @Test
    public void sameKeyDifferentKsRowMutations() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", SameKeyDifferentKsTrigger.class.getName()));
        ColumnFamily cf1 = makeCf(metadata, "k1v1", null);
        ColumnFamily cf2 = makeCf(metadata, "k2v1", null);
        Mutation rm1 = new Mutation(bytes("k1"), cf1);
        Mutation rm2 = new Mutation(bytes("k2"), cf2);

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm1, rm2)));
        assertEquals(4, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        List<ColumnFamily> mutatedCFs = new ArrayList<>(tmutations.get(0).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertEquals(bytes("k1v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")));

        mutatedCFs = new ArrayList<>(tmutations.get(1).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertEquals(bytes("k2v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")));

        mutatedCFs = new ArrayList<>(tmutations.get(2).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")));
        assertEquals(bytes("trigger"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")).value());

        mutatedCFs = new ArrayList<>(tmutations.get(3).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")));
        assertEquals(bytes("trigger"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")).value());
    }

    @Test
    public void differentKeyRowMutations() throws ConfigurationException, InvalidRequestException
    {
        CFMetaData metadata = makeCfMetaData("ks1", "cf1", TriggerDefinition.create("test", DifferentKeyTrigger.class.getName()));
        ColumnFamily cf = makeCf(metadata, "v1", null);
        Mutation rm = new Mutation(UTF8Type.instance.fromString("k1"), cf);

        List<? extends IMutation> tmutations = new ArrayList<>(TriggerExecutor.instance.execute(Arrays.asList(rm)));
        assertEquals(2, tmutations.size());
        Collections.sort(tmutations, new RmComparator());

        assertEquals(bytes("k1"), tmutations.get(0).key());
        assertEquals(bytes("otherKey"), tmutations.get(1).key());

        List<ColumnFamily> mutatedCFs = new ArrayList<>(tmutations.get(0).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertEquals(bytes("v1"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")).value());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")));

        mutatedCFs = new ArrayList<>(tmutations.get(1).getColumnFamilies());
        assertEquals(1, mutatedCFs.size());
        assertNull(mutatedCFs.get(0).getColumn(getColumnName(metadata, "c1")));
        assertEquals(bytes("trigger"), mutatedCFs.get(0).getColumn(getColumnName(metadata, "c2")).value());
    }

    private static CFMetaData makeCfMetaData(String ks, String cf, TriggerDefinition trigger)
    {

        CFMetaData metadata = CFMetaData.sparseCFMetaData(ks, cf, CompositeType.getInstance(UTF8Type.instance));

        metadata.keyValidator(UTF8Type.instance);
        metadata.addOrReplaceColumnDefinition(ColumnDefinition.partitionKeyDef(metadata,
                                                                               UTF8Type.instance.fromString("pkey"),
                                                                               UTF8Type.instance,
                                                                               null));
        metadata.addOrReplaceColumnDefinition(ColumnDefinition.regularDef(metadata,
                                                                          UTF8Type.instance.fromString("c1"),
                                                                          UTF8Type.instance,
                                                                          0));
        metadata.addOrReplaceColumnDefinition(ColumnDefinition.regularDef(metadata,
                                                                          UTF8Type.instance.fromString("c2"),
                                                                          UTF8Type.instance,
                                                                          0));
        try
        {
            if (trigger != null)
                metadata.addTriggerDefinition(trigger);
        }
        catch (InvalidRequestException e)
        {
            throw new AssertionError(e);
        }

        return metadata.rebuild();
    }

    private static ColumnFamily makeCf(CFMetaData metadata, String columnValue1, String columnValue2)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(metadata);

        if (columnValue1 != null)
            cf.addColumn(new BufferCell(getColumnName(metadata, "c1"), bytes(columnValue1)));

        if (columnValue2 != null)
            cf.addColumn(new BufferCell(getColumnName(metadata, "c2"), bytes(columnValue2)));

        return cf;
    }

    private static CellName getColumnName(CFMetaData metadata, String stringName)
    {
        return metadata.comparator.makeCellName(stringName);
    }

    public static class NoOpTrigger implements ITrigger
    {
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            return null;
        }
    }

    public static class SameKeySameCfTrigger implements ITrigger
    {
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(update.metadata());
            cf.addColumn(new BufferCell(getColumnName(update.metadata(), "c2"), bytes("trigger")));
            return Collections.singletonList(new Mutation(update.metadata().ksName, key, cf));
        }
    }

    public static class SameKeySameCfPartialTrigger implements ITrigger
    {
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            if (!key.equals(bytes("k2")))
                return null;

            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(update.metadata());
            cf.addColumn(new BufferCell(getColumnName(update.metadata(), "c2"), bytes("trigger")));
            return Collections.singletonList(new Mutation(update.metadata().ksName, key, cf));
        }
    }

    public static class SameKeyDifferentCfTrigger implements ITrigger
    {
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(makeCfMetaData(update.metadata().ksName, "otherCf", null));
            cf.addColumn(new BufferCell(getColumnName(update.metadata(), "c2"), bytes("trigger")));
            return Collections.singletonList(new Mutation(cf.metadata().ksName, key, cf));
        }
    }

    public static class SameKeyDifferentKsTrigger implements ITrigger
    {
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(makeCfMetaData("otherKs", "otherCf", null));
            cf.addColumn(new BufferCell(getColumnName(update.metadata(), "c2"), bytes("trigger")));
            return Collections.singletonList(new Mutation(cf.metadata().ksName, key, cf));
        }
    }

    public static class DifferentKeyTrigger implements ITrigger
    {
        public Collection<Mutation> augment(ByteBuffer key, ColumnFamily update)
        {
            ColumnFamily cf = ArrayBackedSortedColumns.factory.create(update.metadata());
            cf.addColumn(new BufferCell(getColumnName(update.metadata(), "c2"), bytes("trigger")));
            return Collections.singletonList(new Mutation(cf.metadata().ksName, bytes("otherKey"), cf));
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

    private static class CfComparator implements Comparator<ColumnFamily>
    {
        public int compare(ColumnFamily cf1, ColumnFamily cf2)
        {
            return cf1.metadata().cfName.compareTo(cf2.metadata().cfName);
        }
    }
}
