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

import javax.net.ssl.SSLException;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;
import static org.apache.cassandra.config.CassandraRelevantProperties.DISABLE_TCACTIVE_OPENSSL;

public class PEMBasedSslContextFactoryConfigWithUnencryptedKeysTest
{
    static WithProperties properties;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        properties = new WithProperties()
                     .set(CASSANDRA_CONFIG, "cassandra-pem-sslcontextfactory-unencryptedkeys.yaml")
                     .set(DISABLE_TCACTIVE_OPENSSL, true);
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor()
    {
        properties.close();
    }

    @Test
    public void testHappyPathInlinePEM() throws SSLException
    {

        Config config = DatabaseDescriptor.loadConfig();
        config.client_encryption_options.applyConfig();

        Assert.assertEquals("org.apache.cassandra.security.PEMBasedSslContextFactory",
                            config.client_encryption_options.ssl_context_factory.class_name);
        Assert.assertEquals(config.client_encryption_options.ssl_context_factory.class_name,
                            config.client_encryption_options.sslContextFactoryInstance.getClass().getName());
        PEMBasedSslContextFactory sslContextFactory =
        (PEMBasedSslContextFactory) config.client_encryption_options.sslContextFactoryInstance;
        sslContextFactory.buildKeyManagerFactory();
        sslContextFactory.buildTrustManagerFactory();
    }

    @Test
    public void testHappyPathFileBasedPEM() throws SSLException
    {

        Config config = DatabaseDescriptor.loadConfig();
        config.server_encryption_options.applyConfig();

        Assert.assertEquals("org.apache.cassandra.security.PEMBasedSslContextFactory",
                            config.server_encryption_options.ssl_context_factory.class_name);
        Assert.assertEquals(config.server_encryption_options.ssl_context_factory.class_name,
                            config.server_encryption_options.sslContextFactoryInstance.getClass().getName());
        PEMBasedSslContextFactory sslContextFactory =
        (PEMBasedSslContextFactory) config.server_encryption_options.sslContextFactoryInstance;
        sslContextFactory.buildKeyManagerFactory();
        sslContextFactory.buildTrustManagerFactory();
    }
}
