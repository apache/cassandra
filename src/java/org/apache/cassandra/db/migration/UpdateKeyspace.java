package org.apache.cassandra.db.migration;

import java.io.IOException;

import org.apache.cassandra.config.*;
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
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


public class UpdateKeyspace extends Migration
{
    private KSMetaData newKsm;
    private KSMetaData oldKsm;
    
    /** Required no-arg constructor */
    protected UpdateKeyspace() { }
    
    /** create migration based on thrift parameters */
    public UpdateKeyspace(KSMetaData ksm) throws ConfigurationException, IOException
    {
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getBroadcastAddress()), Schema.instance.getVersion());
        
        assert ksm != null;
        assert ksm.cfMetaData() != null;
        if (ksm.cfMetaData().size() > 0)
            throw new ConfigurationException("Updated keyspace must not contain any column families");
    
        // create the new ksm by merging the one passed in with the cf defs from the exisitng ksm.
        oldKsm = schema.getKSMetaData(ksm.name);
        if (oldKsm == null)
            throw new ConfigurationException(ksm.name + " cannot be updated because it doesn't exist.");

        this.newKsm = KSMetaData.cloneWith(ksm, oldKsm.cfMetaData().values());
        rm = makeDefinitionMutation(newKsm, oldKsm, newVersion);
    }
    
    void applyModels() throws IOException
    {
        schema.clearTableDefinition(oldKsm, newVersion);
        schema.setTableDefinition(newKsm, newVersion);

        Table table = Table.open(newKsm.name, schema);
        try
        {
            table.createReplicationStrategy(newKsm);
        }
        catch (ConfigurationException e)
        {
            throw new IOException(e);
        }

        logger.info("Keyspace updated. Please perform any manual operations.");
    }

    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.UpdateKeyspace uks = new org.apache.cassandra.db.migration.avro.UpdateKeyspace();
        uks.newKs = newKsm.toAvro();
        uks.oldKs = oldKsm.toAvro();
        mi.migration = uks;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.UpdateKeyspace uks = (org.apache.cassandra.db.migration.avro.UpdateKeyspace)mi.migration;
        newKsm = KSMetaData.fromAvro(uks.newKs);
        oldKsm = KSMetaData.fromAvro(uks.oldKs);
    }

    @Override
    public String toString()
    {
        return String.format("Update keyspace %s to %s", oldKsm.toString(), newKsm.toString());
    }
}
