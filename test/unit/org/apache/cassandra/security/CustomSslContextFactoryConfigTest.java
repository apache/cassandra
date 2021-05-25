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
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;

@RunWith(OrderedJUnit4ClassRunner.class)
public class CustomSslContextFactoryConfigTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        System.setProperty("cassandra.config", "cassandra-sslcontextfactory.yaml");
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor() {
        System.clearProperty("cassandra.config");
    }

    @Test
    public void testCustomSslContextFactoryConfiguration() {

        Config config = DatabaseDescriptor.loadConfig();
        config.client_encryption_options.applyConfig();

        Assert.assertEquals("org.apache.cassandra.security.CustomSslContextFactoryImplForTest",
                            config.client_encryption_options.ssl_context_factory.class_name);
        Assert.assertEquals(config.client_encryption_options.ssl_context_factory.class_name,
                            config.client_encryption_options.sslContextFactoryInstance.getClass().getName());
        Assert.assertEquals(3, config.client_encryption_options.ssl_context_factory.parameters.size());
        Assert.assertEquals("value1", config.client_encryption_options.ssl_context_factory.parameters.get("key1"));
        Assert.assertEquals("value2", config.client_encryption_options.ssl_context_factory.parameters.get("key2"));
        Assert.assertEquals("value3", config.client_encryption_options.ssl_context_factory.parameters.get("key3"));
    }
}
