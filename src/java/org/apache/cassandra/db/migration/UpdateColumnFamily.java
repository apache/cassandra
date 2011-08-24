package org.apache.cassandra.db.migration;

import java.io.IOException;

import org.apache.cassandra.config.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.migration.avro.ColumnDef;
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

/** todo: doesn't work with secondary indices yet. See CASSANDRA-1415. */
public class UpdateColumnFamily extends Migration
{
    // does not point to a CFM stored in DatabaseDescriptor.
    private CFMetaData metadata;
    
    protected UpdateColumnFamily() { }
    
    /** assumes validation has already happened. That is, replacing oldCfm with newCfm is neither illegal or totally whackass. */
    public UpdateColumnFamily(org.apache.cassandra.db.migration.avro.CfDef cf_def) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress()), Schema.instance.getVersion());
        
        KSMetaData ksm = schema.getTableDefinition(cf_def.keyspace.toString());
        if (ksm == null)
            throw new ConfigurationException("No such keyspace: " + cf_def.keyspace.toString());
        if (cf_def.column_metadata != null)
        {
            for (ColumnDef entry : cf_def.column_metadata)
            {
                if (entry.index_name != null && !Migration.isLegalName(entry.index_name.toString()))
                    throw new ConfigurationException("Invalid index name: " + entry.index_name);
            }
        }

        CFMetaData oldCfm = schema.getCFMetaData(cf_def.keyspace.toString(), cf_def.name.toString());
        
        // create a copy of the old CF meta data. Apply new settings on top of it.
        this.metadata = CFMetaData.fromAvro(oldCfm.toAvro());
        this.metadata.apply(cf_def);
        
        // create a copy of the old KS meta data. Use it to create a RowMutation that gets applied to schema and migrations.
        KSMetaData newKsMeta = KSMetaData.fromAvro(ksm.toAvro());
        newKsMeta.cfMetaData().get(cf_def.name.toString()).apply(cf_def);
        rm = makeDefinitionMutation(newKsMeta, null, newVersion);
    }

    void applyModels() throws IOException
    {
        logger.debug("Updating " + schema.getCFMetaData(metadata.cfId) + " to " + metadata);
        // apply the meta update.
        try 
        {
            schema.getCFMetaData(metadata.cfId).apply(metadata.toAvro());
        }
        catch (ConfigurationException ex) 
        {
            throw new IOException(ex);
        }
        schema.setTableDefinition(null, newVersion);

        if (!StorageService.instance.isClientMode())
        {
            Table table = Table.open(metadata.ksName, schema);
            ColumnFamilyStore oldCfs = table.getColumnFamilyStore(metadata.cfName);
            oldCfs.reload();
        }
    }

    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.UpdateColumnFamily update = new org.apache.cassandra.db.migration.avro.UpdateColumnFamily();
        update.metadata = metadata.toAvro();
        mi.migration = update;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.UpdateColumnFamily update = (org.apache.cassandra.db.migration.avro.UpdateColumnFamily)mi.migration;
        metadata = CFMetaData.fromAvro(update.metadata);
    }

    @Override
    public String toString()
    {
        return String.format("Update column family to %s", metadata.toString());
    }
}
