package org.apache.cassandra.db.migration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemTable;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.thrift.CfDef;
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

/** todo: doesn't work with secondary indices yet. See CASSANDRA-1415. */
public class UpdateColumnFamily extends Migration
{
    private CFMetaData metadata;
    
    protected UpdateColumnFamily() { }
    
    /** assumes validation has already happened. That is, replacing oldCfm with newCfm is neither illegal or totally whackass. */
    public UpdateColumnFamily(CfDef cf_def) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(cf_def.keyspace);
        if (ksm == null)
            throw new ConfigurationException("Keyspace does not already exist.");
        
        CFMetaData oldCfm = DatabaseDescriptor.getCFMetaData(CFMetaData.getId(cf_def.keyspace, cf_def.name));
        oldCfm.apply(cf_def); 
        this.metadata = oldCfm;
        
        // clone ksm but include the new cf def.
        rm = Migration.makeDefinitionMutation(ksm, null, newVersion);
    }
    
    public void beforeApplyModels()
    {
        if (clientMode)
            return;
        ColumnFamilyStore cfs = Table.open(metadata.tableName).getColumnFamilyStore(metadata.cfName);
        cfs.snapshot(Table.getTimestampedSnapshotName(null));
    }

    void applyModels() throws IOException
    {
        acquireLocks();
        try
        {
            logger.debug("Updating " + metadata + " to " + metadata);
            
            DatabaseDescriptor.setTableDefinition(null, newVersion);
            
            if (!clientMode)
            {
                Table table = Table.open(metadata.tableName);
                
                ColumnFamilyStore oldCfs = table.getColumnFamilyStore(metadata.cfName);
                oldCfs.reload();
            }
        }
        finally
        {
            releaseLocks();
        }
    }

    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.UpdateColumnFamily update = new org.apache.cassandra.db.migration.avro.UpdateColumnFamily();
        update.metadata = metadata.deflate();
        mi.migration = update;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.UpdateColumnFamily update = (org.apache.cassandra.db.migration.avro.UpdateColumnFamily)mi.migration;
        metadata = CFMetaData.inflate(update.metadata);
    }
}
