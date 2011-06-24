package org.apache.cassandra.db.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class DropColumnFamily extends Migration
{
    private String tableName;
    private String cfName;
    
    /** Required no-arg constructor */
    protected DropColumnFamily() { /* pass */ }
    
    public DropColumnFamily(String tableName, String cfName) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.tableName = tableName;
        this.cfName = cfName;
        
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(tableName);
        if (ksm == null)
            throw new ConfigurationException("No such keyspace: " + tableName);
        else if (!ksm.cfMetaData().containsKey(cfName))
            throw new ConfigurationException("CF is not defined in that keyspace.");
        
        KSMetaData newKsm = makeNewKeyspaceDefinition(ksm);
        rm = Migration.makeDefinitionMutation(newKsm, null, newVersion);
    }

    private KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm)
    {
        // clone ksm but do not include the new def
        CFMetaData cfm = ksm.cfMetaData().get(cfName);
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(cfm);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, newCfs.toArray(new CFMetaData[newCfs.size()]));
    }

    public void applyModels() throws IOException
    {
        ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfName);

        // reinitialize the table.
        KSMetaData existing = DatabaseDescriptor.getTableDefinition(tableName);
        CFMetaData cfm = existing.cfMetaData().get(cfName);
        KSMetaData ksm = makeNewKeyspaceDefinition(existing);
        CFMetaData.purge(cfm);
        DatabaseDescriptor.setTableDefinition(ksm, newVersion);

        if (!StorageService.instance.isClientMode())
        {
            cfs.snapshot(Table.getTimestampedSnapshotName(null));

            CompactionManager.instance.getCompactionLock().lock();
            cfs.flushLock.lock();
            try
            {
                Table.open(ksm.name).dropCf(cfm.cfId);
            }
            finally
            {
                cfs.flushLock.unlock();
                CompactionManager.instance.getCompactionLock().unlock();
            }
        }
    }
    
    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.DropColumnFamily dcf = new org.apache.cassandra.db.migration.avro.DropColumnFamily();
        dcf.ksname = new org.apache.avro.util.Utf8(tableName);
        dcf.cfname = new org.apache.avro.util.Utf8(cfName);
        mi.migration = dcf;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.DropColumnFamily dcf = (org.apache.cassandra.db.migration.avro.DropColumnFamily)mi.migration;
        tableName = dcf.ksname.toString();
        cfName = dcf.cfname.toString();
    }

    @Override
    public String toString()
    {
        return String.format("Drop column family: %s.%s", tableName, cfName);
    }
}
