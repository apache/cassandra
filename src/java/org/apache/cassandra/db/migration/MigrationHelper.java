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
package org.apache.cassandra.db.migration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Future;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import org.codehaus.jackson.map.ObjectMapper;

public class MigrationHelper
{
    private static final ObjectMapper jsonMapper = new ObjectMapper();
    private static final Map<Class<?>, Class<?>> primitiveToWrapper = new HashMap<Class<?>, Class<?>>();
    static
    {
        primitiveToWrapper.put(boolean.class, Boolean.class);
        primitiveToWrapper.put(byte.class, Byte.class);
        primitiveToWrapper.put(short.class, Short.class);
        primitiveToWrapper.put(char.class, Character.class);
        primitiveToWrapper.put(int.class, Integer.class);
        primitiveToWrapper.put(long.class, Long.class);
        primitiveToWrapper.put(float.class, Float.class);
        primitiveToWrapper.put(double.class, Double.class);
    }

    public static ByteBuffer readableColumnName(ByteBuffer columnName, AbstractType comparator)
    {
        return ByteBufferUtil.bytes(comparator.getString(columnName));
    }

    public static ByteBuffer valueAsBytes(Object value)
    {
        try
        {
            return ByteBuffer.wrap(jsonMapper.writeValueAsBytes(value));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Object deserializeValue(ByteBuffer value, Class<?> valueClass)
    {
        try
        {
            // because jackson serialized ByteBuffer as byte[] and needs help with deserialization later
            if (valueClass.equals(ByteBuffer.class))
            {
                byte[] bvalue = (byte[]) deserializeValue(value, byte[].class);
                return bvalue == null ? null : ByteBuffer.wrap(bvalue);
            }

            return jsonMapper.readValue(ByteBufferUtil.getArray(value), valueClass);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public static Class<?> getValueClass(Class<?> klass, String name)
    {
        try
        {
            // We want to keep null values, so we must not return a primitive type
            return maybeConvertToWrapperClass(klass.getField(name).getType());
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException(e); // never happens
        }
    }

    private static Class<?> maybeConvertToWrapperClass(Class<?> klass)
    {
        Class<?> cl = primitiveToWrapper.get(klass);
        return cl == null ? klass : cl;
    }

    public static ByteBuffer searchComposite(String comp1, boolean start)
    {
        return compositeNameFor(comp1, !start, null, false, null, false);
    }

    public static ByteBuffer compositeNameFor(String comp1, String comp2)
    {
        return compositeNameFor(comp1, ByteBufferUtil.bytes(comp2), null);
    }

    public static ByteBuffer compositeNameFor(String comp1, ByteBuffer comp2, String comp3)
    {
        return compositeNameFor(comp1, false, comp2, false, comp3, false);
    }

    public static ByteBuffer compositeNameFor(String comp1, boolean limit1, ByteBuffer comp2, boolean limit2, String comp3, boolean limit3)
    {
        int totalSize = 0;

        if (comp1 != null)
            totalSize += 2 + comp1.length() + 1;

        if (comp2 != null)
            totalSize += 2 + comp2.remaining() + 1;

        if (comp3 != null)
            totalSize += 2 + comp3.length() + 1;

        ByteBuffer bytes = ByteBuffer.allocate(totalSize);

        if (comp1 != null)
        {
            bytes.putShort((short) comp1.length());
            bytes.put(comp1.getBytes());
            bytes.put((byte) (limit1 ? 1 : 0));
        }

        if (comp2 != null)
        {
            int pos = comp2.position(), limit = comp2.limit();

            bytes.putShort((short) comp2.remaining());
            bytes.put(comp2);
            bytes.put((byte) (limit2 ? 1 : 0));
            // restore original range
            comp2.position(pos).limit(limit);
        }

        if (comp3 != null)
        {
            bytes.putShort((short) comp3.length());
            bytes.put(comp3.getBytes());
            bytes.put((byte) (limit3 ? 1 : 0));
        }

        bytes.rewind();

        return bytes;
    }

    public static void flushSchemaCFs()
    {
        flushSchemaCF(SystemTable.SCHEMA_KEYSPACES_CF);
        flushSchemaCF(SystemTable.SCHEMA_COLUMNFAMILIES_CF);
        flushSchemaCF(SystemTable.SCHEMA_COLUMNS_CF);
    }

    public static void flushSchemaCF(String cfName)
    {
        Future<?> flush = SystemTable.schemaCFS(cfName).forceFlush();

        if (flush != null)
            FBUtilities.waitOnFuture(flush);
    }

    /* Schema Mutation Helpers */

    public static Collection<RowMutation> addKeyspace(KSMetaData ksm, long timestamp) throws ConfigurationException, IOException
    {
        return addKeyspace(ksm, timestamp, true);
    }

    public static void addKeyspace(KSMetaData ksDef) throws ConfigurationException, IOException
    {
        addKeyspace(ksDef, -1, false);
    }

    public static Collection<RowMutation> addColumnFamily(CFMetaData cfm, long timestamp) throws ConfigurationException, IOException
    {
        return addColumnFamily(cfm, timestamp, true);
    }

    public static void addColumnFamily(CfDef cfDef) throws ConfigurationException, IOException
    {
        try
        {
            addColumnFamily(CFMetaData.fromThrift(cfDef), -1, false);
        }
        catch (InvalidRequestException e)
        {
            throw new ConfigurationException(e.getMessage(), e);
        }
    }

    public static void updateKeyspace(KsDef newState) throws ConfigurationException, IOException
    {
        updateKeyspace(newState, -1, false);
    }

    public static Collection<RowMutation> updateKeyspace(KsDef newState, long timestamp) throws ConfigurationException, IOException
    {
        return updateKeyspace(newState, timestamp, true);
    }

    public static void updateColumnFamily(CfDef newState) throws ConfigurationException, IOException
    {
        updateColumnFamily(newState, -1, false);
    }

    public static Collection<RowMutation> updateColumnFamily(CfDef newState, long timestamp) throws ConfigurationException, IOException
    {
        return updateColumnFamily(newState, timestamp, true);
    }

    public static void dropColumnFamily(String ksName, String cfName) throws IOException
    {
        dropColumnFamily(ksName, cfName, -1, false);
    }

    public static Collection<RowMutation> dropColumnFamily(String ksName, String cfName, long timestamp) throws IOException
    {
        return dropColumnFamily(ksName, cfName, timestamp, true);
    }

    public static void dropKeyspace(String ksName) throws IOException
    {
        dropKeyspace(ksName, -1, false);
    }

    public static Collection<RowMutation> dropKeyspace(String ksName, long timestamp) throws IOException
    {
        return dropKeyspace(ksName, timestamp, true);
    }

    /* Migration Helper implementations */

    private static Collection<RowMutation> addKeyspace(KSMetaData ksm, long timestamp, boolean withSchemaRecord) throws ConfigurationException, IOException
    {
        RowMutation keyspaceDef = ksm.toSchema(timestamp);

        if (withSchemaRecord)
            keyspaceDef.apply();

        Schema.instance.load(ksm);

        if (!StorageService.instance.isClientMode())
            Table.open(ksm.name);

        return toCollection(keyspaceDef);
    }

    private static Collection<RowMutation> addColumnFamily(CFMetaData cfm, long timestamp, boolean withSchemaRecord) throws ConfigurationException, IOException
    {
        KSMetaData ksm = Schema.instance.getTableDefinition(cfm.ksName);
        ksm = KSMetaData.cloneWith(ksm, Iterables.concat(ksm.cfMetaData().values(), Collections.singleton(cfm)));

        Schema.instance.load(cfm);

        RowMutation mutation = null;

        if (withSchemaRecord)
        {
            mutation = cfm.toSchema(timestamp);
            mutation.apply();
        }

        // make sure it's init-ed w/ the old definitions first,
        // since we're going to call initCf on the new one manually
        Table.open(cfm.ksName);

        Schema.instance.setTableDefinition(ksm);

        if (!StorageService.instance.isClientMode())
            Table.open(ksm.name).initCf(cfm.cfId, cfm.cfName);

        return toCollection(mutation);
    }

    private static Collection<RowMutation> updateKeyspace(KsDef newState, long timestamp, boolean withSchemaRecord) throws ConfigurationException, IOException
    {
        KSMetaData oldKsm = Schema.instance.getKSMetaData(newState.name);

        RowMutation schemaUpdate = null;

        if (withSchemaRecord)
        {
            schemaUpdate = oldKsm.diff(newState, timestamp);
            schemaUpdate.apply();
        }

        KSMetaData newKsm = KSMetaData.cloneWith(oldKsm.reloadAttributes(), oldKsm.cfMetaData().values());

        Schema.instance.setTableDefinition(newKsm);

        if (!StorageService.instance.isClientMode())
            Table.open(newState.name).createReplicationStrategy(newKsm);

        return toCollection(schemaUpdate);
    }

    private static Collection<RowMutation> updateColumnFamily(CfDef newState, long timestamp, boolean withSchemaRecord) throws ConfigurationException, IOException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(newState.keyspace, newState.name);

        RowMutation schemaUpdate = null;

        if (withSchemaRecord)
        {
            schemaUpdate = cfm.diff(newState, timestamp);
            schemaUpdate.apply();
        }

        cfm.reload();

        if (!StorageService.instance.isClientMode())
        {
            Table table = Table.open(cfm.ksName);
            table.getColumnFamilyStore(cfm.cfName).reload();
        }

        return toCollection(schemaUpdate);
    }

    private static Collection<RowMutation> dropKeyspace(String ksName, long timestamp, boolean withSchemaRecord) throws IOException
    {
        KSMetaData ksm = Schema.instance.getTableDefinition(ksName);
        String snapshotName = Table.getTimestampedSnapshotName(ksName);

        // remove all cfs from the table instance.
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            ColumnFamilyStore cfs = Table.open(ksm.name).getColumnFamilyStore(cfm.cfName);

            Schema.instance.purge(cfm);

            if (!StorageService.instance.isClientMode())
            {
                cfs.snapshot(snapshotName);
                Table.open(ksm.name).dropCf(cfm.cfId);
            }
        }

        Collection<RowMutation> mutations = Collections.emptyList();

        if (withSchemaRecord)
        {
            mutations = ksm.dropFromSchema(timestamp);
            for (RowMutation m : mutations)
                m.apply();
        }

        // remove the table from the static instances.
        Table.clear(ksm.name);
        Schema.instance.clearTableDefinition(ksm);

        return mutations;
    }

    private static Collection<RowMutation> dropColumnFamily(String ksName, String cfName, long timestamp, boolean withSchemaRecord) throws IOException
    {
        KSMetaData ksm = Schema.instance.getTableDefinition(ksName);
        ColumnFamilyStore cfs = Table.open(ksName).getColumnFamilyStore(cfName);

        // reinitialize the table.
        CFMetaData cfm = ksm.cfMetaData().get(cfName);

        Schema.instance.purge(cfm);
        Schema.instance.setTableDefinition(makeNewKeyspaceDefinition(ksm, cfm));

        RowMutation mutation = null;

        if (withSchemaRecord)
        {
            mutation = cfm.dropFromSchema(timestamp);
            mutation.apply();
        }

        if (!StorageService.instance.isClientMode())
        {
            cfs.snapshot(Table.getTimestampedSnapshotName(cfs.columnFamily));
            Table.open(ksm.name).dropCf(cfm.cfId);
        }

        return toCollection(mutation);
    }

    private static KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm, CFMetaData toExclude)
    {
        // clone ksm but do not include the new def
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(toExclude);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        return KSMetaData.cloneWith(ksm, newCfs);
    }

    private static Collection<RowMutation> toCollection(RowMutation mutation)
    {
        return mutation == null ? Collections.<RowMutation>emptyList() : Collections.singleton(mutation);
    }
}
