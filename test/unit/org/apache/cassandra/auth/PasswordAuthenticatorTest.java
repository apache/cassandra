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
import java.util.Map;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.mindrot.jbcrypt.BCrypt;

import com.datastax.driver.core.Authenticator;
import com.datastax.driver.core.EndPoint;
import com.datastax.driver.core.PlainTextAuthProvider;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.cql3.validation.entities.TimeuuidTest;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.AuthMetricsManager;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.auth.AuthTestUtils.ALL_ROLES;
import static org.apache.cassandra.auth.CassandraRoleManager.DEFAULT_SUPERUSER_PASSWORD;
import static org.apache.cassandra.auth.CassandraRoleManager.getGensaltLogRounds;
import static org.apache.cassandra.auth.PasswordAuthenticator.AuthEnforcementFlagEnum;
import static org.apache.cassandra.auth.PasswordAuthenticator.EMPTY_PWD_USERNAME;
import static org.apache.cassandra.auth.PasswordAuthenticator.EMPTY_USER_USERNAME;
import static org.apache.cassandra.auth.PasswordAuthenticator.SaslNegotiator;
import static org.apache.cassandra.auth.PasswordAuthenticator.checkpw;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mindrot.jbcrypt.BCrypt.gensalt;
import static org.mindrot.jbcrypt.BCrypt.hashpw;

import static org.apache.cassandra.auth.CassandraRoleManager.GENSALT_LOG2_ROUNDS_PROPERTY;

@RunWith(Parameterized.class)
public class PasswordAuthenticatorTest extends CQLTester
{
    private final static PasswordAuthenticator authenticator = new PasswordAuthenticator();
    private static final boolean authEnabled = DatabaseDescriptor.getAuthenticator().requireAuthentication();
    AuthEnforcementFlagEnum authEnforcementFlag;
    long empty_pwd_failure_count;
    AuthenticationException authException, expectedException;
    AuthenticationException baseAuthException = new AuthenticationException("Authentication exception");

    public PasswordAuthenticatorTest(AuthEnforcementFlagEnum authEnforcementFlag)
    {
        authenticator.setAuthEnforcementFlag(authEnforcementFlag);
        this.authEnforcementFlag = authEnforcementFlag;

        if (authEnforcementFlag == AuthEnforcementFlagEnum.NONE) {
            empty_pwd_failure_count = 0;
            expectedException = baseAuthException;
        } else if (authEnforcementFlag == AuthEnforcementFlagEnum.HARD) {
            empty_pwd_failure_count = 1;
            expectedException = baseAuthException;
        } else {
            empty_pwd_failure_count = 1;
            expectedException = null;
        }
    }

    @Parameterized.Parameters()
    public static List<AuthEnforcementFlagEnum> generateData()
    {
        return Arrays.asList(AuthEnforcementFlagEnum.NONE, AuthEnforcementFlagEnum.HARD, AuthEnforcementFlagEnum.SOFT);
    }

    @BeforeClass
    public static void setupClass() throws Exception
    {
        SchemaLoader.loadSchema();
        DatabaseDescriptor.daemonInitialization();
        StorageService.instance.initServer(0);
        authenticator.setup();
    }

    @Before
    public void setup() throws Exception
    {
        ColumnFamilyStore.getIfExists(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES).truncateBlocking();
        ColumnFamilyStore.getIfExists(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLE_MEMBERS).truncateBlocking();
        resetGcGraceSeconds();
    }

    @Test
    public void testCheckpw()
    {
        // Valid and correct
        assertTrue(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(getGensaltLogRounds()))));
        assertTrue(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(4))));
        assertTrue(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw(DEFAULT_SUPERUSER_PASSWORD, gensalt(12))));

        // Valid but incorrect hashes
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw("incorrect0", gensalt(4))));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw("incorrect1", gensalt(10))));
        assertFalse(checkpw(DEFAULT_SUPERUSER_PASSWORD, hashpw("incorrect2", gensalt(12))));

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

    @Test(expected = ConfigurationException.class)
    public void testInvalidUpperBoundHashingRoundsValue()
    {
        executeSaltRoundsPropertyTest(31);
    }

    @Test(expected = ConfigurationException.class)
    public void testInvalidLowerBoundHashingRoundsValue()
    {
        executeSaltRoundsPropertyTest(3);
    }

    private void executeSaltRoundsPropertyTest(Integer rounds)
    {
        String oldProperty = System.getProperty(GENSALT_LOG2_ROUNDS_PROPERTY);
        try
        {
            System.setProperty(GENSALT_LOG2_ROUNDS_PROPERTY, rounds.toString());
            getGensaltLogRounds();
            Assert.fail("Property " + GENSALT_LOG2_ROUNDS_PROPERTY + " must be in interval [4,30]");
        }
        finally
        {
            if (oldProperty != null)
                System.setProperty(GENSALT_LOG2_ROUNDS_PROPERTY, oldProperty);
            else
                System.clearProperty(GENSALT_LOG2_ROUNDS_PROPERTY);
        }
    }

    @Test
    public void testEmptyUsername() {
        authException = null;
        try {
            testDecodeIllegalUserAndPwd("", "pwd");
        } catch (AuthenticationException e) {
            authException = baseAuthException;
        }
        assertEquals(expectedException, authException);
    }

    @Test
    public void testEmptyUsername01()
    {
        authException = null;
        long empty_pwd_failure_count_old = AuthMetricsManager.getMetrics(EMPTY_USER_USERNAME, authEnabled, authEnforcementFlag.name()).userFailureMetrics.getCount();
        try {
            testDecodeIllegalUserAndPwd("", "pwd");
        } catch (AuthenticationException e) {
            authException = baseAuthException;
            long empty_pwd_failure_count_new = AuthMetricsManager.getMetrics(EMPTY_USER_USERNAME, authEnabled, authEnforcementFlag.name()).userFailureMetrics.getCount();
            assertEquals(empty_pwd_failure_count_new - empty_pwd_failure_count_old,empty_pwd_failure_count);
        }
        assertEquals(expectedException, authException);
    }

    @Test
    public void testEmptyPassword()
    {
        authException = null;
        try {
            testDecodeIllegalUserAndPwd("user", "");
        } catch (AuthenticationException e) {
            authException = baseAuthException;
        }
        assertEquals(expectedException, authException);
    }

    @Test
    public void testEmptyPassword01()
    {
        authException = null;
        long empty_pwd_failure_count_old = AuthMetricsManager.getMetrics(EMPTY_PWD_USERNAME, authEnabled, authEnforcementFlag.name()).userFailureMetrics.getCount();
        try {
            testDecodeIllegalUserAndPwd("user", "");
        } catch (AuthenticationException e) {
            authException = baseAuthException;
            long empty_pwd_failure_count_new = AuthMetricsManager.getMetrics(EMPTY_PWD_USERNAME, authEnabled, authEnforcementFlag.name()).userFailureMetrics.getCount();
            assertEquals(empty_pwd_failure_count_new - empty_pwd_failure_count_old, empty_pwd_failure_count);
        }
        assertEquals(expectedException, authException);
    }

    @Test
    public void testNULUsername0() {
        authException = null;
        try {
            byte[] user = {'u', 's', PasswordAuthenticator.NUL, 'e', 'r'};
            testDecodeIllegalUserAndPwd(new String(user, StandardCharsets.UTF_8), "pwd");
        } catch (AuthenticationException e) {
            authException = baseAuthException;
        }
        assertEquals(expectedException, authException);
    }

    @Test
    public void testNULUsername1(){
        authException = null;
        try {
            testDecodeIllegalUserAndPwd(new String(new byte[4]), "pwd");
        } catch (AuthenticationException e) {
            authException = baseAuthException;
        }
        assertEquals(expectedException, authException);
    }

    @Test
    public void testNULPassword0(){
        authException = null;
        try {
            byte[] pwd = {'p', 'w', PasswordAuthenticator.NUL, 'd'};
            testDecodeIllegalUserAndPwd("user", new String(pwd, StandardCharsets.UTF_8));
        } catch (AuthenticationException e) {
            authException = baseAuthException;
        }
        assertEquals(expectedException, authException);
    }

    @Test
    public void testNULPassword1() {
        authException = null;
        try {
        testDecodeIllegalUserAndPwd("user", "");
        } catch (AuthenticationException e) {
            authException = baseAuthException;
        }
        assertEquals(expectedException, authException);
    }

    public void resetGcGraceSeconds() {
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        InetAddressAndPort local = FBUtilities.getBroadcastAddressAndPort();
        tokenMeta.clearUnsafe();
        tokenMeta.updateHostId(UUID.randomUUID(), local);
        tokenMeta.updateNormalTokens(BootStrapper.getRandomTokens(tokenMeta, 1), local);

        for (TableMetadata table : Schema.instance.getTablesAndViews("system_auth"))
            table.unbuild().gcGraceSeconds(864000).build();
    }

    private void testDecodeIllegalUserAndPwd(String username, String password)
    {
        SaslNegotiator negotiator = authenticator.newSaslNegotiator(null);
        Authenticator clientAuthenticator = (new PlainTextAuthProvider(username, password))
                                            .newAuthenticator((EndPoint) null, null);

        negotiator.evaluateResponse(clientAuthenticator.initialResponse());
        negotiator.getAuthenticatedUser();
    }

    @Test
    public void warmCacheLoadsAllEntriesFromTables()
    {
        IRoleManager roleManager = new AuthTestUtils.LocalCassandraRoleManager();
        roleManager.setup();
        for (RoleResource r : ALL_ROLES)
        {
            RoleOptions options = new RoleOptions();
            options.setOption(IRoleManager.Option.PASSWORD, "hash_for_" + r.getRoleName());
            roleManager.createRole(AuthenticatedUser.ANONYMOUS_USER, r, options);
        }

        Map<String, String> cacheEntries = authenticator.bulkLoader().get();

        assertEquals(ALL_ROLES.length, cacheEntries.size());
        cacheEntries.forEach((username, hash) -> assertTrue(BCrypt.checkpw("hash_for_" + username, hash)));
    }

    @Test
    public void warmCacheWithEmptyTable()
    {
        Map<String, String> cacheEntries = authenticator.bulkLoader().get();
        assertTrue(cacheEntries.isEmpty());
    }
}
