package org.apache.cassandra.db.migration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.Iterables;

import org.apache.cassandra.config.*;
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


public class AddColumnFamily extends Migration
{
    private CFMetaData cfm;
    
    /** Required no-arg constructor */
    protected AddColumnFamily() { /* pass */ }
    
    public AddColumnFamily(CFMetaData cfm) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress()), Schema.instance.getVersion());
        this.cfm = cfm;
        KSMetaData ksm = schema.getTableDefinition(cfm.ksName);
        
        if (ksm == null)
            throw new ConfigurationException("No such keyspace: " + cfm.ksName);
        else if (ksm.cfMetaData().containsKey(cfm.cfName))
            throw new ConfigurationException(String.format("%s already exists in keyspace %s",
                                                           cfm.cfName,
                                                           cfm.ksName));
        else if (!Migration.isLegalName(cfm.cfName))
            throw new ConfigurationException("Invalid column family name: " + cfm.cfName);
        for (Map.Entry<ByteBuffer, ColumnDefinition> entry : cfm.getColumn_metadata().entrySet())
        {
            String indexName = entry.getValue().getIndexName();
            if (indexName != null && !Migration.isLegalName(indexName))
                throw new ConfigurationException("Invalid index name: " + indexName);
        }

        // clone ksm but include the new cf def.
        KSMetaData newKsm = makeNewKeyspaceDefinition(ksm);
        
        rm = makeDefinitionMutation(newKsm, null, newVersion);
    }
    
    private KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm)
    {
        return KSMetaData.cloneWith(ksm, Iterables.concat(ksm.cfMetaData().values(), Collections.singleton(cfm)));
    }
    
    public void applyModels() throws IOException
    {
        // reinitialize the table.
        KSMetaData ksm = schema.getTableDefinition(cfm.ksName);
        ksm = makeNewKeyspaceDefinition(ksm);
        try
        {
            schema.load(cfm);
        }
        catch (ConfigurationException ex)
        {
            throw new IOException(ex);
        }
        Table.open(cfm.ksName, schema); // make sure it's init-ed w/ the old definitions first, since we're going to call initCf on the new one manually
        schema.setTableDefinition(ksm, newVersion);
        // these definitions could have come from somewhere else.
        schema.fixCFMaxId();
        if (!StorageService.instance.isClientMode())
            Table.open(ksm.name, schema).initCf(cfm.cfId, cfm.cfName);
    }

    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.AddColumnFamily acf = new org.apache.cassandra.db.migration.avro.AddColumnFamily();
        acf.cf = cfm.toAvro();
        mi.migration = acf;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.AddColumnFamily acf = (org.apache.cassandra.db.migration.avro.AddColumnFamily)mi.migration;
        cfm = CFMetaData.fromAvro(acf.cf);
    }

    @Override
    public String toString()
    {
        return "Add column family: " + cfm.toString();
    }
}
