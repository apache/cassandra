/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.config;

import static org.junit.Assert.assertNotNull;

import org.apache.avro.specific.SpecificRecord;

import org.apache.cassandra.CleanupHelper;
import org.apache.cassandra.db.migration.AddKeyspace;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.io.SerDeUtils;

import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

public class DatabaseDescriptorTest
{
    protected <D extends SpecificRecord> D serDe(D record, D newInstance) throws IOException
    {
        D actual = SerDeUtils.deserialize(record.getSchema(),
                                              SerDeUtils.serialize(record),
                                              newInstance);
        assert actual.equals(record) : actual + " != " + record;
        return actual;
    }
    
    @Test
    public void testCFMetaDataSerialization() throws IOException, ConfigurationException
    {
        // test serialization of all defined test CFs.
        for (String table : Schema.instance.getNonSystemTables())
        {
            for (CFMetaData cfm : Schema.instance.getTableMetaData(table).values())
            {
                CFMetaData cfmDupe = CFMetaData.fromAvro(serDe(cfm.toAvro(), new org.apache.cassandra.db.migration.avro.CfDef()));
                assert cfmDupe != null;
                assert cfmDupe.equals(cfm);
            }
        }
    }

    @Test
    public void testKSMetaDataSerialization() throws IOException, ConfigurationException
    {
        for (KSMetaData ksm : Schema.instance.getTableDefinitions())
        {
            // Not testing round-trip on the KsDef via serDe() because maps
            //  cannot be compared in avro.
            KSMetaData ksmDupe = KSMetaData.fromAvro(ksm.toAvro());
            assert ksmDupe != null;
            assert ksmDupe.equals(ksm);
        }
    }
    
    // this came as a result of CASSANDRA-995
    @Test
    public void testTransKsMigration() throws IOException, ConfigurationException
    {
        CleanupHelper.cleanupAndLeaveDirs();
        DatabaseDescriptor.loadSchemas();
        assert Schema.instance.getNonSystemTables().size() == 0;
        
        // add a few.
        AddKeyspace ks0 = new AddKeyspace(KSMetaData.testMetadata("ks0", SimpleStrategy.class, KSMetaData.optsWithRF(3)));
        ks0.apply();
        AddKeyspace ks1 = new AddKeyspace(KSMetaData.testMetadata("ks1", SimpleStrategy.class, KSMetaData.optsWithRF(3)));
        ks1.apply();

        assert Schema.instance.getTableDefinition("ks0") != null;
        assert Schema.instance.getTableDefinition("ks1") != null;

        Schema.instance.clearTableDefinition(Schema.instance.getTableDefinition("ks0"), new UUID(4096, 0));
        Schema.instance.clearTableDefinition(Schema.instance.getTableDefinition("ks1"), new UUID(4096, 0));

        assert Schema.instance.getTableDefinition("ks0") == null;
        assert Schema.instance.getTableDefinition("ks1") == null;
        
        DatabaseDescriptor.loadSchemas();

        assert Schema.instance.getTableDefinition("ks0") != null;
        assert Schema.instance.getTableDefinition("ks1") != null;
    }
}
