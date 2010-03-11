/**
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

package org.apache.cassandra.db;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.filter.SliceQueryFilter;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.config.DatabaseDescriptor.ConfigurationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class DefsTable
{
    public static final String MIGRATIONS_CF = "Migrations";
    public static final String SCHEMA_CF = "Schema";

    /** add a column family. */
    public static synchronized void add(CFMetaData cfm) throws IOException, ConfigurationException
    {
        Table.openLock.lock();
        try
        {
            // make sure the ks is real and the cf doesn't already exist.
            KSMetaData ksm = DatabaseDescriptor.getTableDefinition(cfm.tableName);
            if (ksm == null)
                throw new ConfigurationException("Keyspace does not already exist.");
            else if (ksm.cfMetaData().containsKey(cfm.cfName))
                throw new ConfigurationException("CF is already defined in that keyspace.");

            // clone ksm but include the new cf def.
            List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
            newCfs.add(cfm);
            ksm = new KSMetaData(ksm.name, ksm.strategyClass, ksm.replicationFactor, ksm.snitch, newCfs.toArray(new CFMetaData[newCfs.size()]));

            // store it.
            UUID newVersion = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
            RowMutation rm = new RowMutation(Table.DEFINITIONS, newVersion.toString());
            rm.add(new QueryPath(SCHEMA_CF, null, ksm.name.getBytes()), KSMetaData.serialize(ksm), System.currentTimeMillis());
            rm.apply();

            // reinitialize the table.
            DatabaseDescriptor.setTableDefinition(ksm, newVersion);
            Table.reinitialize(ksm.name);
            
            // force creation of a new commit log segment.
            CommitLog.instance().forceNewSegment();
        }
        finally
        {
            Table.openLock.unlock();
        }
    }

    /**
     * drop a column family. blockOnDeletion was added to make testing simpler.
     */
    public static synchronized void drop(CFMetaData cfm, boolean blockOnDeletion) throws IOException, ConfigurationException
    {
        Table.openLock.lock();
        try
        {
            KSMetaData ksm = DatabaseDescriptor.getTableDefinition(cfm.tableName);
            if (ksm == null)
                throw new ConfigurationException("Keyspace does not already exist.");
            else if (!ksm.cfMetaData().containsKey(cfm.cfName))
                throw new ConfigurationException("CF is not defined in that keyspace.");
            
            // clone ksm but do not include the new def
            List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
            newCfs.remove(cfm);
            assert newCfs.size() == ksm.cfMetaData().size() - 1;
            ksm = new KSMetaData(ksm.name, ksm.strategyClass, ksm.replicationFactor, ksm.snitch, newCfs.toArray(new CFMetaData[newCfs.size()]));
            
            // store it.
            UUID newVersion = UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress());
            RowMutation rm = new RowMutation(Table.DEFINITIONS, newVersion.toString());
            rm.add(new QueryPath(SCHEMA_CF, null, ksm.name.getBytes()), KSMetaData.serialize(ksm), System.currentTimeMillis());
            rm.apply();
            
            // reinitialize the table.
            CFMetaData.purge(cfm);
            DatabaseDescriptor.setTableDefinition(ksm, newVersion);
            Table.reinitialize(ksm.name);
            
            // indicate that some files need to be deleted (eventually)
            SystemTable.markForRemoval(cfm);
            
            // we don't really need a new segment, but let's force it to be consistent with other operations.
            CommitLog.instance().forceNewSegment();
        }
        finally
        {
            Table.openLock.unlock();
        }
        
        if (blockOnDeletion)
        {
            // notify the compaction manager that it needs to clean up the dropped cf files.
            try
            {
                CompactionManager.instance.submitGraveyardCleanup().get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    public static synchronized void dumpToStorage(UUID version) throws IOException
    {
        String versionKey = version.toString();
        long now = System.currentTimeMillis();
        RowMutation rm = new RowMutation(Table.DEFINITIONS, versionKey);
        for (String tableName : DatabaseDescriptor.getNonSystemTables())
        {
            KSMetaData ks = DatabaseDescriptor.getTableDefinition(tableName);
            rm.add(new QueryPath(SCHEMA_CF, null, ks.name.getBytes()), KSMetaData.serialize(ks), now);
        }
        rm.apply();
    }

    public static synchronized Collection<KSMetaData> loadFromStorage(UUID version) throws IOException
    {
        Table defs = Table.open(Table.DEFINITIONS);
        ColumnFamilyStore cfStore = defs.getColumnFamilyStore(SCHEMA_CF);
        SliceQueryFilter filter = new SliceQueryFilter(version.toString(), new QueryPath(SCHEMA_CF), "".getBytes(), "".getBytes(), false, 1024);
        ColumnFamily cf = cfStore.getColumnFamily(filter);
        Collection<KSMetaData> tables = new ArrayList<KSMetaData>();
        for (IColumn col : cf.getSortedColumns())
        {
            String ksName = new String(col.name());
            KSMetaData ks = KSMetaData.deserialize(new ByteArrayInputStream(col.value()));
            tables.add(ks);
        }
        return tables;
    }
}
