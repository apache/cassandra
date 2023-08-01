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
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_CONFIG;

public class PEMBasedSslContextFactoryConfigWithMismatchingPasswordsTest
{
    static WithProperties properties;

    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        properties = new WithProperties().set(CASSANDRA_CONFIG, "cassandra-pem-sslcontextfactory-mismatching-passwords.yaml");
    }

    @AfterClass
    public static void tearDownDatabaseDescriptor()
    {
        properties.close();
    }

    @Test(expected = ConfigurationException.class)
    public void testInLinePEMConfiguration() throws SSLException
    {
        Config config = DatabaseDescriptor.loadConfig();
        try
        {
            config.client_encryption_options.applyConfig();
        }
        catch (ConfigurationException e)
        {
            assertErrorMessageAndRethrow(e);
        }
    }

    @Test(expected = ConfigurationException.class)
    public void testFileBasedPEMConfiguration() throws SSLException
    {
        Config config = DatabaseDescriptor.loadConfig();
        try
        {
            config.server_encryption_options.applyConfig();
        }
        catch (ConfigurationException e)
        {
            assertErrorMessageAndRethrow(e);
        }
    }

    private void assertErrorMessageAndRethrow(ConfigurationException e) throws ConfigurationException
    {
        String expectedMessage = "'keystore_password' and 'key_password' both configurations are given and the values do not match";
        Throwable rootCause = getRootCause(e);
        String actualMessage = rootCause.getMessage();
        Assert.assertEquals(expectedMessage, actualMessage);
        throw e;
    }

    private Throwable getRootCause(Throwable e)
    {
        Throwable rootCause = e;
        while (rootCause.getCause() != null && rootCause.getCause() != rootCause)
        {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }
}
