package org.apache.cassandra.db.migration;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
    private boolean blockOnFileDeletion;
    
    /** Required no-arg constructor */
    protected DropColumnFamily() { /* pass */ }
    
    public DropColumnFamily(String tableName, String cfName, boolean blockOnFileDeletion) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.tableName = tableName;
        this.cfName = cfName;
        this.blockOnFileDeletion = blockOnFileDeletion;
        
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(tableName);
        if (ksm == null)
            throw new ConfigurationException("Keyspace does not already exist.");
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
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.replicationFactor, newCfs.toArray(new CFMetaData[newCfs.size()]));
    }

    @Override
    public void beforeApplyModels()
    {
        if (clientMode)
            return;
        ColumnFamilyStore cfs = Table.open(tableName).getColumnFamilyStore(cfName);
        cfs.snapshot(Table.getTimestampedSnapshotName(null));
    }

    @Override
    public void applyModels() throws IOException
    {
        // reinitialize the table.
        KSMetaData existing = DatabaseDescriptor.getTableDefinition(tableName);
        CFMetaData cfm = existing.cfMetaData().get(cfName);
        KSMetaData ksm = makeNewKeyspaceDefinition(existing);
        CFMetaData.purge(cfm);
        DatabaseDescriptor.setTableDefinition(ksm, newVersion);
        
        if (!clientMode)
        {
            Table.open(ksm.name).dropCf(cfm.cfId);
        }
    }
    
    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.DropColumnFamily dcf = new org.apache.cassandra.db.migration.avro.DropColumnFamily();
        dcf.ksname = new org.apache.avro.util.Utf8(tableName);
        dcf.cfname = new org.apache.avro.util.Utf8(cfName);
        dcf.block_on_deletion = blockOnFileDeletion;
        mi.migration = dcf;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.DropColumnFamily dcf = (org.apache.cassandra.db.migration.avro.DropColumnFamily)mi.migration;
        tableName = dcf.ksname.toString();
        cfName = dcf.cfname.toString();
        blockOnFileDeletion = dcf.block_on_deletion;
    }
}
