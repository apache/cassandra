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
package org.apache.cassandra.auth;


import java.nio.charset.StandardCharsets;

import com.google.common.collect.Iterables;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.PlainTextAuthProvider;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.schema.KeyspaceParams;

import static org.apache.cassandra.auth.CassandraRoleManager.*;
import static org.apache.cassandra.auth.PasswordAuthenticator.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mindrot.jbcrypt.BCrypt.hashpw;
import static org.mindrot.jbcrypt.BCrypt.gensalt;

public class PasswordAuthenticatorTest extends CQLTester
{

    private static PasswordAuthenticator authenticator = new PasswordAuthenticator();

    @Test
    public void testCheckpw() throws Exception
    {
        // Valid and correct
        assertTrue(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(getGensaltLogRounds()))));
        assertTrue(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(4))));
        assertTrue(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(31))));

        // Valid but incorrect hashes
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw("incorrect0", gensalt(4))));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw("incorrect1", gensalt(10))));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw("incorrect2", gensalt(31))));

        // Invalid hash values, the jBCrypt library implementation
        // throws an exception which we catch and treat as a failure
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, ""));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, "0"));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD,
                            "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"));

        // Format is structurally right, but actually invalid
        // bad salt version
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, "$5x$10$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234"));
        // invalid number of rounds, multiple salt versions but it's the rounds that are incorrect
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, "$2$02$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234"));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, "$2a$02$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234"));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, "$2$99$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234"));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, "$2a$99$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234"));
        // unpadded rounds
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, "$2$6$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234"));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, "$2a$6$abcdefghijklmnopqrstuvABCDEFGHIJKLMNOPQRSTUVWXYZ01234"));
    }

    @Test(expected = AuthenticationException.class)
    public void testEmptyUsername()
    {
        testDecodeIllegalUserAndPwd("", "pwd");
    }

    @Test(expected = AuthenticationException.class)
    public void testEmptyPassword()
    {
        testDecodeIllegalUserAndPwd("user", "");
    }

    @Test(expected = AuthenticationException.class)
    public void testNULUsername0()
    {
        byte[] user = {'u', 's', PasswordAuthenticator.NUL, 'e', 'r'};
        testDecodeIllegalUserAndPwd(new String(user, StandardCharsets.UTF_8), "pwd");
    }

    @Test(expected = AuthenticationException.class)
    public void testNULUsername1()
    {
        testDecodeIllegalUserAndPwd(new String(new byte[4]), "pwd");
    }

    @Test(expected = AuthenticationException.class)
    public void testNULPassword0()
    {
        byte[] pwd = {'p', 'w', PasswordAuthenticator.NUL, 'd'};
        testDecodeIllegalUserAndPwd("user", new String(pwd, StandardCharsets.UTF_8));
    }

    @Test(expected = AuthenticationException.class)
    public void testNULPassword1()
    {
        testDecodeIllegalUserAndPwd("user", new String(new byte[4]));
    }

    private void testDecodeIllegalUserAndPwd(String username, String password)
    {
        SaslNegotiator negotiator = authenticator.newSaslNegotiator(null);
        Authenticator clientAuthenticator = (new PlainTextAuthProvider(username, password))
                                            .newAuthenticator(null, null);

        negotiator.evaluateResponse(clientAuthenticator.initialResponse());
        negotiator.getAuthenticatedUser();
    }

    @BeforeClass
    public static void setUp()
    {
        SchemaLoader.createKeyspace(SchemaConstants.AUTH_KEYSPACE_NAME,
                                    KeyspaceParams.simple(1),
                                    Iterables.toArray(AuthKeyspace.metadata().tables, CFMetaData.class));
        authenticator.setup();
    }

    @AfterClass
    public static void tearDown()
    {
        schemaChange("DROP KEYSPACE " + SchemaConstants.AUTH_KEYSPACE_NAME);
    }
}