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

import java.io.DataOutput;
import java.io.DataOutputStream;
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
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

public class MigrationHelper
{
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

    public static ByteBuffer searchComposite(String name, boolean start)
    {
        assert name != null;
        ByteBuffer nameBytes = UTF8Type.instance.decompose(name);
        int length = nameBytes.remaining();
        byte[] bytes = new byte[2 + length + 1];
        bytes[0] = (byte)((length >> 8) & 0xFF);
        bytes[1] = (byte)(length & 0xFF);
        ByteBufferUtil.arrayCopy(nameBytes, 0, bytes, 2, length);
        bytes[bytes.length - 1] = (byte)(start ? 0 : 1);
        return ByteBuffer.wrap(bytes);
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

    /* Migration Helper implementations */

    public static RowMutation addKeyspace(KSMetaData ksm, long timestamp, boolean withSchemaRecord) throws ConfigurationException, IOException
    {
        RowMutation mutation = null;

        if (withSchemaRecord)
        {
            mutation = ksm.toSchema(timestamp);
            mutation.apply();
        }

        Schema.instance.load(ksm);

        if (!StorageService.instance.isClientMode())
            Table.open(ksm.name);

        return mutation;
    }

    public static RowMutation addColumnFamily(CFMetaData cfm, long timestamp, boolean withSchemaRecord) throws ConfigurationException, IOException
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

        return mutation;
    }

    public static RowMutation updateKeyspace(KSMetaData newState, long timestamp, boolean withSchemaRecord) throws ConfigurationException, IOException
    {
        KSMetaData oldKsm = Schema.instance.getKSMetaData(newState.name);

        RowMutation mutation = null;

        if (withSchemaRecord)
        {
            mutation = oldKsm.diff(newState, timestamp);
            mutation.apply();
        }

        KSMetaData newKsm = KSMetaData.cloneWith(oldKsm.reloadAttributes(), oldKsm.cfMetaData().values());

        Schema.instance.setTableDefinition(newKsm);

        if (!StorageService.instance.isClientMode())
            Table.open(newState.name).createReplicationStrategy(newKsm);

        return mutation;
    }

    public static RowMutation updateColumnFamily(CFMetaData newState, long timestamp, boolean withSchemaRecord) throws ConfigurationException, IOException
    {
        CFMetaData cfm = Schema.instance.getCFMetaData(newState.ksName, newState.cfName);

        RowMutation mutation = null;

        if (withSchemaRecord)
        {
            mutation = cfm.diff(newState, timestamp);
            mutation.apply();
        }

        cfm.reload();

        if (!StorageService.instance.isClientMode())
        {
            Table table = Table.open(cfm.ksName);
            table.getColumnFamilyStore(cfm.cfName).reload();
        }

        return mutation;
    }

    public static RowMutation dropKeyspace(String ksName, long timestamp, boolean withSchemaRecord) throws IOException
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

        RowMutation mutation = null;

        if (withSchemaRecord)
        {
            mutation = ksm.dropFromSchema(timestamp);
            mutation.apply();
        }

        // remove the table from the static instances.
        Table.clear(ksm.name);
        Schema.instance.clearTableDefinition(ksm);

        return mutation;
    }

    public static RowMutation dropColumnFamily(String ksName, String cfName, long timestamp, boolean withSchemaRecord) throws IOException
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

        return mutation;
    }

    private static KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm, CFMetaData toExclude)
    {
        // clone ksm but do not include the new def
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(toExclude);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        return KSMetaData.cloneWith(ksm, newCfs);
    }
}
