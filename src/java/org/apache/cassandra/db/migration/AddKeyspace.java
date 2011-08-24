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
import org.apache.cassandra.db.Table;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class AddKeyspace extends Migration
{
    private KSMetaData ksm;
    
    /** Required no-arg constructor */
    protected AddKeyspace() { /* pass */ }
    
    public AddKeyspace(KSMetaData ksm) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress()), Schema.instance.getVersion());
        
        if (schema.getTableDefinition(ksm.name) != null)
            throw new ConfigurationException("Keyspace already exists.");
        if (!Migration.isLegalName(ksm.name))
            throw new ConfigurationException("Invalid keyspace name: " + ksm.name);
        for (CFMetaData cfm : ksm.cfMetaData().values())
            if (!Migration.isLegalName(cfm.cfName))
                throw new ConfigurationException("Invalid column family name: " + cfm.cfName);
        
        this.ksm = ksm;
        rm = makeDefinitionMutation(ksm, null, newVersion);
    }

    public void applyModels() throws IOException
    {
        for (CFMetaData cfm : ksm.cfMetaData().values())
        {
            try
            {
                schema.load(cfm);
            }
            catch (ConfigurationException ex)
            {
                // throw RTE since this indicates a table,cf maps to an existing ID. It shouldn't if this is really a
                // new keyspace.
                throw new RuntimeException(ex);
            }
        }
        schema.setTableDefinition(ksm, newVersion);
        // these definitions could have come from somewhere else.
        schema.fixCFMaxId();
        if (!StorageService.instance.isClientMode())
        {
            Table.open(ksm.name, schema);
        }
    }
    
    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.AddKeyspace aks = new org.apache.cassandra.db.migration.avro.AddKeyspace();
        aks.ks = ksm.toAvro();
        mi.migration = aks;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.AddKeyspace aks = (org.apache.cassandra.db.migration.avro.AddKeyspace)mi.migration;
        ksm = KSMetaData.fromAvro(aks.ks);
    }

    @Override
    public String toString()
    {
        return "Add keyspace: " + ksm.toString();
    }
}
