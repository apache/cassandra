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

package org.apache.cassandra.db.migration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/** Deprecated until we can figure out how to rename on a live node without complicating flushing and compaction. */
@Deprecated
public class RenameKeyspace extends Migration
{
    private String oldName;
    private String newName;
    
    /** Required no-arg constructor */
    protected RenameKeyspace() { /* pass */ }
    
    public RenameKeyspace(String oldName, String newName) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.oldName = oldName;
        this.newName = newName;
        
        KSMetaData oldKsm = DatabaseDescriptor.getTableDefinition(oldName);
        if (oldKsm == null)
            throw new ConfigurationException("Keyspace either does not exist or does not match the one currently defined.");
        if (DatabaseDescriptor.getTableDefinition(newName) != null)
            throw new ConfigurationException("Keyspace already exists.");
        if (!Migration.isLegalName(newName))
            throw new ConfigurationException("Invalid keyspace name: " + newName);
        
        // clone the ksm, replacing thename.
        KSMetaData newKsm = rename(oldKsm, newName, false); 
        
        rm = makeDefinitionMutation(newKsm, oldKsm, newVersion);
    }
    
    private static KSMetaData rename(KSMetaData ksm, String newName, boolean purgeOldCfs)
    {
        // cfs will need to have their tablenames reset. CFMetaData are immutable, so new ones get created with the
        // same ids.
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().size());
        for (CFMetaData oldCf : ksm.cfMetaData().values())
        {
            if (purgeOldCfs)
                CFMetaData.purge(oldCf);
            newCfs.add(CFMetaData.renameTable(oldCf, newName));
        }
        return new KSMetaData(newName, ksm.strategyClass, ksm.strategyOptions, newCfs.toArray(new CFMetaData[newCfs.size()]));
    }

    public void applyModels() throws IOException
    {
        if (!StorageService.instance.isClientMode())
            renameKsStorageFiles(oldName, newName);
        
        KSMetaData oldKsm = DatabaseDescriptor.getTableDefinition(oldName);
        for (CFMetaData cfm : oldKsm.cfMetaData().values())
            CFMetaData.purge(cfm);
        KSMetaData newKsm = rename(oldKsm, newName, true);
        for (CFMetaData cfm : newKsm.cfMetaData().values())
        {
            try
            {
                CFMetaData.map(cfm);
            }
            catch (ConfigurationException ex)
            {
                // throwing RTE since this this means that the table,cf already maps to a different ID, which it can't
                // since we've already checked for an existing table with the same name.
                throw new RuntimeException(ex);
            }
        }
        // ^^ at this point, the static methods in CFMetaData will start returning references to the new table, so
        // it helps if the node is reasonably quiescent with respect to this ks.
        DatabaseDescriptor.clearTableDefinition(oldKsm, newVersion);
        DatabaseDescriptor.setTableDefinition(newKsm, newVersion);
        
        if (!StorageService.instance.isClientMode())
        {
            Table.clear(oldKsm.name);
            Table.open(newName);
        }
    }
    
    private static void renameKsStorageFiles(String oldKs, String newKs) throws IOException
    {
        ArrayList<File> failed = new ArrayList<File>();
        for (String dataDir : DatabaseDescriptor.getAllDataFileLocations())
        {
            File ksDir = new File(dataDir, oldKs);
            if (ksDir.exists())
                if (!ksDir.renameTo(new File(dataDir, newKs)))
                    failed.add(ksDir);
        }

        if (!failed.isEmpty())
            throw new IOException("One or more problems encountered while renaming " + StringUtils.join(failed, ","));
    }
    
    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.RenameKeyspace rks = new org.apache.cassandra.db.migration.avro.RenameKeyspace();
        rks.old_ksname = new org.apache.avro.util.Utf8(oldName);
        rks.new_ksname = new org.apache.avro.util.Utf8(newName);
        mi.migration = rks;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.RenameKeyspace rks = (org.apache.cassandra.db.migration.avro.RenameKeyspace)mi.migration;
        oldName = rks.old_ksname.toString();
        newName = rks.new_ksname.toString();
    }

    @Override
    public String toString()
    {
        return String.format("Rename keyspace %s to %s", oldName, newName);
    }
}
