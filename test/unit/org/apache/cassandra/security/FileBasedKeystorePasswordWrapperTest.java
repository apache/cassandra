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

import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileBasedKeystorePasswordWrapperTest
{
    /**
     * Tests default behavior without keystore password file specified.
     */
    @Test
    public void testPasswordConfiguration()
    {
        FileBasedKeystorePasswordWrapper wrapper = new FileBasedKeystorePasswordWrapper("test/conf/cassandra_ssl_test.keystore", "cassandra", null);
        assertEquals("Password must be loaded from the direct configuration", "cassandra", wrapper.getPassword());
    }

    /**
     * Tests behavior when password for keystore is specified via a password file.
     */
    @Test
    public void testPasswordFileConfiguration()
    {
        FileBasedKeystorePasswordWrapper wrapper = new FileBasedKeystorePasswordWrapper("test/conf/cassandra_ssl_test.keystore", null, "test/conf/cassandra_ssl_test.keystore.passwordfile");
        assertEquals("Password must be loaded from the password file", "cassandra", wrapper.getPassword());
    }

    /**
     * Tests when password for keystore is specified via password configuration and a password file both.
     */
    @Test
    public void testPasswordAndPasswordFileCOnfiguration()
    {
        FileBasedKeystorePasswordWrapper wrapper = new FileBasedKeystorePasswordWrapper("test/conf/cassandra_ssl_test.keystore", "cassandra123", "test/conf/cassandra_ssl_test.keystore.passwordfile");
        assertTrue("Password file must take precedence", wrapper.isPasswordFileUsed());
    }

    /**
     * Tests behavior when a non-existing password file is specified for keystore's password and password configuration
     * is {@code null}.
     */
    @Test(expected = ConfigurationException.class)
    public void testMissingPasswordFile()
    {
        new FileBasedKeystorePasswordWrapper("test/conf/cassandra_ssl_test.keystore", "", "passwordfile-that-doesnotexist");
    }
}
