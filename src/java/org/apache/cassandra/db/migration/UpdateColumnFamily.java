package org.apache.cassandra.db.migration;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
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

/** todo: doesn't work with secondary indices yet. See CASSANDRA-1415. */
public class UpdateColumnFamily extends Migration
{
    private CFMetaData oldCfm;
    private CFMetaData newCfm;
    
    protected UpdateColumnFamily() { }
    
    /** assumes validation has already happened. That is, replacing oldCfm with newCfm is neither illegal or totally whackass. */
    public UpdateColumnFamily(CFMetaData oldCfm, CFMetaData newCfm) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(newCfm.tableName);
        if (ksm == null)
            throw new ConfigurationException("Keyspace does not already exist.");
        
        this.oldCfm = oldCfm;
        this.newCfm = newCfm;
        
        // we'll allow this eventually.
        if (!oldCfm.column_metadata.equals(newCfm.column_metadata))
            throw new ConfigurationException("Column meta information is not identical.");
        
        // clone ksm but include the new cf def.
        KSMetaData newKsm = makeNewKeyspaceDefinition(ksm);
        rm = Migration.makeDefinitionMutation(newKsm, null, newVersion);
    }
    
    private KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm)
    {
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(oldCfm);
        newCfs.add(newCfm);
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.replicationFactor, newCfs.toArray(new CFMetaData[newCfs.size()]));
    }
    
    public void beforeApplyModels()
    {
        if (clientMode)
            return;
        ColumnFamilyStore cfs = Table.open(oldCfm.tableName).getColumnFamilyStore(oldCfm.cfName);
        cfs.snapshot(Table.getTimestampedSnapshotName(null));
    }

    void applyModels() throws IOException
    {
        // all we really need to do is reload the cfstore.
        KSMetaData newKsm = makeNewKeyspaceDefinition(DatabaseDescriptor.getTableDefinition(newCfm.tableName));
        DatabaseDescriptor.setTableDefinition(newKsm, newVersion);
        
        if (!clientMode)
            Table.open(oldCfm.tableName).reloadCf(newCfm.cfId);
    }

    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.UpdateColumnFamily update = new org.apache.cassandra.db.migration.avro.UpdateColumnFamily();
        update.newCf = newCfm.deflate();
        update.oldCf = oldCfm.deflate();
        mi.migration = update;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.UpdateColumnFamily update = (org.apache.cassandra.db.migration.avro.UpdateColumnFamily)mi.migration;
        newCfm = CFMetaData.inflate(update.newCf);
        oldCfm = CFMetaData.inflate(update.oldCf);
    }
}
