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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class DropKeyspace extends Migration
{
    private String name;
    
    /** Required no-arg constructor */
    protected DropKeyspace() { /* pass */ }
    
    public DropKeyspace(String name) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.name = name;
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(name);
        if (ksm == null)
            throw new ConfigurationException("Keyspace does not exist.");
        rm = makeDefinitionMutation(null, ksm, newVersion);
    }

    @Override
    public void beforeApplyModels()
    {
        if (!clientMode)
            Table.open(name).snapshot(null);
    }

    @Override
    public void applyModels() throws IOException
    {
        acquireLocks();
        try
        {
            KSMetaData ksm = DatabaseDescriptor.getTableDefinition(name);
            // remove the table from the static instances.
            Table table = Table.clear(ksm.name);
            if (table == null)
                throw new IOException("Table is not active. " + ksm.name);
            
            // remove all cfs from the table instance.
            for (CFMetaData cfm : ksm.cfMetaData().values())
            {
                CFMetaData.purge(cfm);
                if (!clientMode)
                {
                    table.dropCf(cfm.cfId);
                }
            }
                            
            // reset defs.
            DatabaseDescriptor.clearTableDefinition(ksm, newVersion);
            
            if (!clientMode)
            {
                // clear up any local hinted data for this keyspace.
                HintedHandOffManager.renameHints(name, null);
            }
        }
        finally
        {
            releaseLocks();
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
