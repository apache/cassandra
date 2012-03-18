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

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.InvalidRequestException;

import org.junit.Test;

import java.io.IOException;

public class DatabaseDescriptorTest
{
    @Test
    public void testCFMetaDataSerialization() throws IOException, ConfigurationException, InvalidRequestException
    {
        // test serialization of all defined test CFs.
        for (String table : Schema.instance.getNonSystemTables())
        {
            for (CFMetaData cfm : Schema.instance.getTableMetaData(table).values())
            {
                CFMetaData cfmDupe = CFMetaData.fromThrift(cfm.toThrift());
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
            KSMetaData ksmDupe = KSMetaData.fromThrift(ksm.toThrift());
            assert ksmDupe != null;
            assert ksmDupe.equals(ksm);
        }
    }

    // this came as a result of CASSANDRA-995
    @Test
    public void testTransKsMigration() throws IOException, ConfigurationException
    {
        SchemaLoader.cleanupAndLeaveDirs();
        DatabaseDescriptor.loadSchemas();
        assert Schema.instance.getNonSystemTables().size() == 0;

        Gossiper.instance.start((int)(System.currentTimeMillis() / 1000));

        try
        {
            // add a few.
            MigrationManager.announceNewKeyspace(KSMetaData.testMetadata("ks0", SimpleStrategy.class, KSMetaData.optsWithRF(3)));
            MigrationManager.announceNewKeyspace(KSMetaData.testMetadata("ks1", SimpleStrategy.class, KSMetaData.optsWithRF(3)));

            assert Schema.instance.getTableDefinition("ks0") != null;
            assert Schema.instance.getTableDefinition("ks1") != null;

            Schema.instance.clearTableDefinition(Schema.instance.getTableDefinition("ks0"));
            Schema.instance.clearTableDefinition(Schema.instance.getTableDefinition("ks1"));

            assert Schema.instance.getTableDefinition("ks0") == null;
            assert Schema.instance.getTableDefinition("ks1") == null;

            DatabaseDescriptor.loadSchemas();

            assert Schema.instance.getTableDefinition("ks0") != null;
            assert Schema.instance.getTableDefinition("ks1") != null;
        }
        finally
        {
            Gossiper.instance.stop();
        }
    }
}
