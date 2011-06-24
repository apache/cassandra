package org.apache.cassandra.db.migration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
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

/** Deprecated until we can figure out how to rename on a live node without complicating flushing and compaction. */
@Deprecated
public class RenameColumnFamily extends Migration
{
    private String tableName;
    private String oldName;
    private String newName;
    private Integer cfId;
    
    /** Required no-arg constructor */
    protected RenameColumnFamily() { /* pass */ }
    
    // this this constructor sets the cfid, it can only be called form a node that is starting the migration. It cannot
    // be called during deserialization of this migration.
    public RenameColumnFamily(String tableName, String oldName, String newName) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.tableName = tableName;
        this.oldName = oldName;
        this.newName = newName;
        
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(tableName);
        if (ksm == null)
            throw new ConfigurationException("No such keyspace: " + tableName);
        if (!ksm.cfMetaData().containsKey(oldName))
            throw new ConfigurationException("CF is not defined in that keyspace.");
        if (ksm.cfMetaData().containsKey(newName))
            throw new ConfigurationException("CF is already defined in that keyspace.");
        if (!Migration.isLegalName(newName))
            throw new ConfigurationException("Invalid column family name: " + newName);
        
        cfId = ksm.cfMetaData().get(oldName).cfId;
        
        // clone the ksm, replacing cfm with the new one.
        KSMetaData newKsm = makeNewKeyspaceDefinition(ksm);
        rm = Migration.makeDefinitionMutation(newKsm, null, newVersion);
    }
    
    private KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm)
    {
        CFMetaData oldCfm = ksm.cfMetaData().get(oldName);
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.remove(oldCfm);
        assert newCfs.size() == ksm.cfMetaData().size() - 1;
        CFMetaData newCfm = CFMetaData.rename(oldCfm, newName);
        newCfs.add(newCfm);
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, newCfs.toArray(new CFMetaData[newCfs.size()]));
    }

    public void applyModels() throws IOException
    {
        // leave it up to operators to ensure there are no writes going on durng the file rename. Just know that
        // attempting row mutations on oldcfName right now would be really bad.
        
        // reset defs.
        KSMetaData oldKsm = DatabaseDescriptor.getTableDefinition(tableName);
        CFMetaData.purge(oldKsm.cfMetaData().get(oldName));
        KSMetaData ksm = makeNewKeyspaceDefinition(DatabaseDescriptor.getTableDefinition(tableName));
        try 
        {
            CFMetaData.map(ksm.cfMetaData().get(newName));
        }
        catch (ConfigurationException ex)
        {
            // throwing RTE since this this means that the table,cf already maps to a different ID, which has already
            // been checked in the constructor and shouldn't happen.
            throw new RuntimeException(ex);
        }
        DatabaseDescriptor.setTableDefinition(ksm, newVersion);
        
        if (!StorageService.instance.isClientMode())
        {
            Table.open(ksm.name).renameCf(cfId, newName);
        }
    }
    
    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.RenameColumnFamily rcf = new org.apache.cassandra.db.migration.avro.RenameColumnFamily();
        rcf.ksname = new org.apache.avro.util.Utf8(tableName);
        rcf.cfid = cfId;
        rcf.old_cfname = new org.apache.avro.util.Utf8(oldName);
        rcf.new_cfname = new org.apache.avro.util.Utf8(newName);
        mi.migration = rcf;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.RenameColumnFamily rcf = (org.apache.cassandra.db.migration.avro.RenameColumnFamily)mi.migration;
        tableName = rcf.ksname.toString();
        cfId = rcf.cfid;
        oldName = rcf.old_cfname.toString();
        newName = rcf.new_cfname.toString();
    }

    @Override
    public String toString()
    {
        return String.format("Rename column family (%d) %s.%s to %s.%s", cfId, tableName, oldName, tableName, newName);
    }
}
