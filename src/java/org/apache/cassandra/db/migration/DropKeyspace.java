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

import java.io.IOException;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class DropKeyspace extends Migration
{
    private String name;
    
    /** Required no-arg constructor */
    protected DropKeyspace() { /* pass */ }
    
    public DropKeyspace(String name) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress()), Schema.instance.getVersion());
        this.name = name;
        KSMetaData ksm = schema.getTableDefinition(name);
        if (ksm == null)
            throw new ConfigurationException("Keyspace does not exist.");
        rm = makeDefinitionMutation(null, ksm, newVersion);
    }

    public void applyModels() throws IOException
    {
        String snapshotName = Table.getTimestampedSnapshotName(name);
        CompactionManager.instance.getCompactionLock().lock();
        try
        {
            KSMetaData ksm = schema.getTableDefinition(name);

            // remove all cfs from the table instance.
            for (CFMetaData cfm : ksm.cfMetaData().values())
            {
                ColumnFamilyStore cfs = Table.open(ksm.name, schema).getColumnFamilyStore(cfm.cfName);
                schema.purge(cfm);
                if (!StorageService.instance.isClientMode())
                {
                    cfs.snapshot(snapshotName);
                    cfs.flushLock.lock();
                    try
                    {
                        Table.open(ksm.name, schema).dropCf(cfm.cfId);
                    }
                    finally
                    {
                        cfs.flushLock.unlock();
                    }
                }
            }
                            
            // remove the table from the static instances.
            Table.clear(ksm.name, schema);
            // reset defs.
            schema.clearTableDefinition(ksm, newVersion);
        }
        finally
        {
            CompactionManager.instance.getCompactionLock().unlock();
        }
    }
    
    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.DropKeyspace dks = new org.apache.cassandra.db.migration.avro.DropKeyspace();
        dks.ksname = new org.apache.avro.util.Utf8(name);
        mi.migration = dks;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.DropKeyspace dks = (org.apache.cassandra.db.migration.avro.DropKeyspace)mi.migration;
        name = dks.ksname.toString();
    }
    
    @Override
    public String toString()
    {
        return "Drop keyspace: " + name;
    }
}
