/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.security;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;

public class CustomSslContextFactoryConfigTest
{
    static WithProperties properties;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        properties = new WithProperties().set(CASSANDRA_CONFIG, "cassandra-sslcontextfactory.yaml");
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor() {
        properties.close();
    }

    @Test
    public void testValidCustomSslContextFactoryConfiguration() {

        Config config = DatabaseDescriptor.loadConfig();
        config.client_encryption_options.applyConfig();

        Assert.assertEquals("org.apache.cassandra.security.DummySslContextFactoryImpl",
                            config.client_encryption_options.ssl_context_factory.class_name);
        Assert.assertEquals(config.client_encryption_options.ssl_context_factory.class_name,
                            config.client_encryption_options.sslContextFactoryInstance.getClass().getName());
        Assert.assertEquals(3, config.client_encryption_options.ssl_context_factory.parameters.size());
        Assert.assertEquals("value1", config.client_encryption_options.ssl_context_factory.parameters.get("key1"));
        Assert.assertEquals("value2", config.client_encryption_options.ssl_context_factory.parameters.get("key2"));
        Assert.assertEquals("value3", config.client_encryption_options.ssl_context_factory.parameters.get("key3"));
        DummySslContextFactoryImpl dummySslContextFactory =
        (DummySslContextFactoryImpl)config.client_encryption_options.sslContextFactoryInstance;
        Assert.assertEquals("dummy-keystore",dummySslContextFactory.getStringValueFor("keystore"));
    }

    @Test
    public void testInvalidCustomSslContextFactoryConfiguration() {
        Config config = DatabaseDescriptor.loadConfig();
        try {
            config.server_encryption_options.applyConfig();
        } catch(ConfigurationException ce) {
            Assert.assertEquals("Unable to create instance of ISslContextFactory for org.apache.cassandra.security" +
                                ".MissingSslContextFactoryImpl", ce.getMessage());
            Assert.assertNotNull("Unable to find root cause of pluggable ISslContextFactory loading failure",
                                 ce.getCause());
            Assert.assertTrue(ce.getCause() instanceof ClassNotFoundException);
        }
    }
}
