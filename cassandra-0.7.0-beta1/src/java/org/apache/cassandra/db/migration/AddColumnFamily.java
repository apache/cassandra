package org.apache.cassandra.db.migration;

import org.apache.avro.Schema;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.io.SerDeUtils;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
        super(UUIDGen.makeType1UUIDFromHost(FBUtilities.getLocalAddress()), DatabaseDescriptor.getDefsVersion());
        this.cfm = cfm;
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(cfm.tableName);
        
        if (ksm == null)
            throw new ConfigurationException("Keyspace does not already exist.");
        else if (ksm.cfMetaData().containsKey(cfm.cfName))
            throw new ConfigurationException("CF is already defined in that keyspace.");
        
        // clone ksm but include the new cf def.
        KSMetaData newKsm = makeNewKeyspaceDefinition(ksm);
        
        rm = Migration.makeDefinitionMutation(newKsm, null, newVersion);
    }
    
    private KSMetaData makeNewKeyspaceDefinition(KSMetaData ksm)
    {
        List<CFMetaData> newCfs = new ArrayList<CFMetaData>(ksm.cfMetaData().values());
        newCfs.add(cfm);
        return new KSMetaData(ksm.name, ksm.strategyClass, ksm.strategyOptions, ksm.replicationFactor, newCfs.toArray(new CFMetaData[newCfs.size()]));
    }
    
    public void applyModels() throws IOException
    {
        // reinitialize the table.
        KSMetaData ksm = DatabaseDescriptor.getTableDefinition(cfm.tableName);
        ksm = makeNewKeyspaceDefinition(ksm);
        try
        {
            CFMetaData.map(cfm);
        }
        catch (ConfigurationException ex)
        {
            throw new IOException(ex);
        }
        Table.open(cfm.tableName); // make sure it's init-ed w/ the old definitions first, since we're going to call initCf on the new one manually
        DatabaseDescriptor.setTableDefinition(ksm, newVersion);
        if (!clientMode)
            Table.open(ksm.name).initCf(cfm.cfId, cfm.cfName);

        if (!clientMode)
            // force creation of a new commit log segment.
            CommitLog.instance().forceNewSegment();
    }

    public void subdeflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.AddColumnFamily acf = new org.apache.cassandra.db.migration.avro.AddColumnFamily();
        acf.cf = cfm.deflate();
        mi.migration = acf;
    }

    public void subinflate(org.apache.cassandra.db.migration.avro.Migration mi)
    {
        org.apache.cassandra.db.migration.avro.AddColumnFamily acf = (org.apache.cassandra.db.migration.avro.AddColumnFamily)mi.migration;
        cfm = CFMetaData.inflate(acf.cf);
    }
}
