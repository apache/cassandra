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

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;

import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;


import java.io.IOException;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DatabaseDescriptorTest
{
    @Test
    public void testCFMetaDataSerialization() throws IOException, ConfigurationException, InvalidRequestException
    {
        // test serialization of all defined test CFs.
        for (String keyspaceName : Schema.instance.getNonSystemKeyspaces())
        {
            for (CFMetaData cfm : Schema.instance.getKeyspaceMetaData(keyspaceName).values())
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
        for (KSMetaData ksm : Schema.instance.getKeyspaceDefinitions())
        {
            // Not testing round-trip on the KsDef via serDe() because maps
            KSMetaData ksmDupe = KSMetaData.fromThrift(ksm.toThrift());
            assert ksmDupe != null;
            assert ksmDupe.equals(ksm);
        }
    }

    // this came as a result of CASSANDRA-995
    @Test
    public void testTransKsMigration() throws ConfigurationException
    {
        SchemaLoader.cleanupAndLeaveDirs();
        DatabaseDescriptor.loadSchemas();
        assert Schema.instance.getNonSystemKeyspaces().size() == 0;

        Gossiper.instance.start((int)(System.currentTimeMillis() / 1000));

        try
        {
            // add a few.
            MigrationManager.announceNewKeyspace(KSMetaData.testMetadata("ks0", SimpleStrategy.class, KSMetaData.optsWithRF(3)));
            MigrationManager.announceNewKeyspace(KSMetaData.testMetadata("ks1", SimpleStrategy.class, KSMetaData.optsWithRF(3)));

            assert Schema.instance.getKSMetaData("ks0") != null;
            assert Schema.instance.getKSMetaData("ks1") != null;

            Schema.instance.clearKeyspaceDefinition(Schema.instance.getKSMetaData("ks0"));
            Schema.instance.clearKeyspaceDefinition(Schema.instance.getKSMetaData("ks1"));

            assert Schema.instance.getKSMetaData("ks0") == null;
            assert Schema.instance.getKSMetaData("ks1") == null;

            DatabaseDescriptor.loadSchemas();

            assert Schema.instance.getKSMetaData("ks0") != null;
            assert Schema.instance.getKSMetaData("ks1") != null;
        }
        finally
        {
            Gossiper.instance.stop();
        }
    }

    @Test
    public void testConfigurationLoader() throws Exception
    {
        // By default, we should load from the yaml
        Config config = DatabaseDescriptor.loadConfig();
        assertEquals("Test Cluster", config.cluster_name);

        // Now try custom loader
        ConfigurationLoader testLoader = new TestLoader();
        System.setProperty("cassandra.config.loader", testLoader.getClass().getName());

        config = DatabaseDescriptor.loadConfig();
        assertEquals("ConfigurationLoader Test", config.cluster_name);
    }

    public static class TestLoader implements ConfigurationLoader
    {
        public Config loadConfig() throws ConfigurationException
        {
            Config testConfig = new Config();
            testConfig.cluster_name = "ConfigurationLoader Test";;
            return testConfig;
        }
    }
}
