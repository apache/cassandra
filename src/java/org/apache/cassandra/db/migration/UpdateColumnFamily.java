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
        logger.debug("Updating " + oldCfm + " to " + newCfm);
        KSMetaData newKsm = makeNewKeyspaceDefinition(DatabaseDescriptor.getTableDefinition(newCfm.tableName));
        DatabaseDescriptor.setTableDefinition(newKsm, newVersion);
        
        if (!clientMode)
        {
            Table table = Table.open(oldCfm.tableName);
            ColumnFamilyStore oldCfs = table.getColumnFamilyStore(oldCfm.cfName);
            table.reloadCf(newCfm.cfId);

            // clean up obsolete index data files
            for (Map.Entry<ByteBuffer, ColumnDefinition> entry : oldCfm.column_metadata.entrySet())
            {
                ByteBuffer column = entry.getKey();
                ColumnDefinition def = entry.getValue();
                if (def.index_type != null
                    && (!newCfm.column_metadata.containsKey(column) || newCfm.column_metadata.get(column).index_type == null))
                {
                    ColumnFamilyStore indexCfs = oldCfs.getIndexedColumnFamilyStore(column);
                    SystemTable.setIndexRemoved(table.name, indexCfs.columnFamily);
                    indexCfs.removeAllSSTables();
                }
            }
        }
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
