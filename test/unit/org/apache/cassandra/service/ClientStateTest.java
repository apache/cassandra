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

package org.apache.cassandra.service;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.util.concurrent.Uninterruptibles;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AllowAllAuthenticator;
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.config.CassandraRelevantProperties.ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClientStateTest
{
    static WithProperties properties;

    @BeforeClass
    public static void beforeClass()
    {
        properties = new WithProperties().set(ORG_APACHE_CASSANDRA_DISABLE_MBEAN_REGISTRATION, true);
        SchemaLoader.prepareServer();
        DatabaseDescriptor.setAuthFromRoot(true);
        DatabaseDescriptor.setRoleManager(new AuthTestUtils.LocalCassandraRoleManager());
        DatabaseDescriptor.getRoleManager().setup();
        Roles.init();
        AuthCacheService.initializeAndRegisterCaches();
    }

    @AfterClass
    public static void afterClass()
    {
        properties.close();
    }

    @Test
    public void permissionsCheckStartsAtHeadOfResourceChain()
    {
        // verify that when performing a permissions check, we start from the
        // root IResource in the applicable hierarchy and proceed to the more
        // granular resources until we find the required permission (or until
        // we reach the end of the resource chain). This is because our typical
        // usage is to grant blanket permissions on the root resources to users
        // and so we save lookups, cache misses and cache space by traversing in
        // this order. e.g. for DataResources, we typically grant perms on the
        // 'data' resource, so when looking up a users perms on a specific table
        // it makes sense to follow: data -> keyspace -> table

        final AtomicInteger getPermissionsRequestCount = new AtomicInteger(0);
        final IResource rootResource = DataResource.root();
        final IResource tableResource = DataResource.table("test_ks", "test_table");
        final AuthenticatedUser testUser = new AuthenticatedUser("test_user")
        {
            public Set<Permission> getPermissions(IResource resource)
            {
                getPermissionsRequestCount.incrementAndGet();
                if (resource.equals(rootResource))
                    return Permission.ALL;

                fail(String.format("Permissions requested for unexpected resource %s", resource));
                // need a return to make the compiler happy
                return null;
            }

            public boolean canLogin() { return true; }
        };

        Roles.cache.invalidate();

        // finally, need to configure CassandraAuthorizer so we don't shortcircuit out of the authz process
        DatabaseDescriptor.setAuthorizer(new AuthTestUtils.LocalCassandraAuthorizer());

        // check permissions on the table, which should check for the root resource first
        // & return successfully without needing to proceed further
        ClientState state = ClientState.forInternalCalls();
        state.login(testUser);
        state.ensurePermission(Permission.SELECT, tableResource);
        assertEquals(1, getPermissionsRequestCount.get());
    }

    @Test
    public void superuserStatusIsMemoized()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(60_000);

            CountingUser user = new CountingUser(true);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            assertTrue(state.isSuper());
            assertEquals(1, user.superuserChecks);

            assertTrue(state.isSuper());
            assertTrue(state.cloneWithKeyspaceIfSet(SchemaConstants.SYSTEM_KEYSPACE_NAME).isSuper());
            assertEquals(1, user.superuserChecks); // no extra user.isSuper invocations

        });
    }

    @Test
    public void memoizedSuperuserStatusExpiresWithRolesValidity()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(200);

            CountingUser user = new CountingUser(false);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            assertFalse(state.isSuper());
            assertEquals(1, user.superuserChecks);

            user.isSuper = true;
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            assertTrue(state.isSuper());
            assertEquals(2, user.superuserChecks);
        });
    }

    @Test
    public void memoizedSuperuserStatusIsDiscardedWhenRolesCacheIsInvalidated()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(60_000);

            CountingUser user = new CountingUser(false);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            assertFalse(state.isSuper());
            user.isSuper = true;
            assertFalse(state.isSuper());
            assertEquals(1, user.superuserChecks);

            Roles.cache.invalidate();

            assertTrue(state.isSuper());
            assertEquals(2, user.superuserChecks);
        });
    }

    @Test
    public void memoizedSuperuserStatusIsDiscardedWhenRolesValidityIsReconfigured()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(60_000);

            CountingUser user = new CountingUser(false);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            assertFalse(state.isSuper());
            user.isSuper = true;
            assertEquals(1, user.superuserChecks);

            Roles.cache.setValidity(30_000);

            assertTrue(state.isSuper());
            assertEquals(2, user.superuserChecks);
        });
    }

    @Test
    public void superuserStatusIsNotMemoizedWhenRolesCacheIsDisabled()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(0);

            CountingUser user = new CountingUser(true);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            assertTrue(state.isSuper());
            assertEquals(1, user.superuserChecks);

            assertTrue(state.isSuper());
            assertEquals(2, user.superuserChecks);
        });
    }

    @Test
    public void loginDiscardsTheMemoizedSuperuserStatus()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(60_000);

            CountingUser superuser = new CountingUser(true);
            ClientState state = ClientState.forInternalCalls();
            state.login(superuser);

            assertTrue(state.isSuper());
            assertEquals(1, superuser.superuserChecks);

            CountingUser ordinary = new CountingUser(false);
            state.login(ordinary);

            assertFalse(state.isSuper());
            assertEquals(1, ordinary.superuserChecks);
        });
    }

    @Test
    public void memoizedSuperuserStatusExpiresWithRolesUpdateInterval()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(60_000);
            DatabaseDescriptor.setRolesUpdateInterval(200);

            CountingUser user = new CountingUser(false);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            assertFalse(state.isSuper());
            assertEquals(1, user.superuserChecks);

            user.isSuper = true;
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            assertTrue(state.isSuper());
            assertEquals(2, user.superuserChecks);
        });
    }

    @Test
    public void memoizedSuperuserStatusIsDiscardedWhenRolesUpdateIntervalIsReconfigured()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(60_000);

            CountingUser user = new CountingUser(false);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            assertFalse(state.isSuper());
            user.isSuper = true;
            assertFalse(state.isSuper());
            assertEquals(1, user.superuserChecks);

            Roles.cache.setUpdateInterval(30_000);

            assertTrue(state.isSuper());
            assertEquals(2, user.superuserChecks);
        });
    }

    @Test
    public void memoizedSuperuserStatusIsDiscardedWhenValidityIsReduced()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(60_000);
            DatabaseDescriptor.setRolesUpdateInterval(60_000);

            CountingUser user = new CountingUser(false);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            long generationBefore = Roles.cacheGeneration();

            assertFalse(state.isSuper());
            user.isSuper = true;
            assertFalse(state.isSuper());
            assertEquals(1, user.superuserChecks);

            DatabaseDescriptor.setRolesValidity(30_000);
            assertEquals("expect same generation", generationBefore, Roles.cacheGeneration());

            assertTrue("super user state changed", state.isSuper());
            assertEquals(2, user.superuserChecks);
        });
    }

    @Test
    public void memoizedSuperuserStatusDoesntChangeWhenValidityIsExtended()
    {
        withAuthenticationRequired(() -> {
            DatabaseDescriptor.setRolesValidity(60_000);
            DatabaseDescriptor.setRolesUpdateInterval(60_000);

            CountingUser user = new CountingUser(false);
            ClientState state = ClientState.forInternalCalls();
            state.login(user);

            assertFalse(state.isSuper());
            user.isSuper = true;
            assertEquals(1, user.superuserChecks);

            long generationBefore = Roles.cacheGeneration();
            DatabaseDescriptor.setRolesValidity(600_000);
            DatabaseDescriptor.setRolesUpdateInterval(600_000);
            assertEquals("the roles cache generation must not have moved", generationBefore, Roles.cacheGeneration());

            assertFalse("still memoized since the validity of the cache increased", state.isSuper());
            assertEquals(1, user.superuserChecks);
        });
    }

    private static void withAuthenticationRequired(Runnable test)
    {
        IAuthenticator previousAuthenticator = DatabaseDescriptor.getAuthenticator();
        int previousRolesValidity = DatabaseDescriptor.getRolesValidity();
        try
        {
            DatabaseDescriptor.setAuthenticator(new AllowAllAuthenticator()
            {
                @Override
                public boolean requireAuthentication()
                {
                    return true;
                }
            });
            test.run();
        }
        finally
        {
            DatabaseDescriptor.setAuthenticator(previousAuthenticator);
            DatabaseDescriptor.setRolesValidity(previousRolesValidity);
            DatabaseDescriptor.setRolesUpdateInterval(-1);
            Roles.cache.invalidate();
        }
    }

    private static class CountingUser extends AuthenticatedUser
    {
        private int superuserChecks;
        private boolean isSuper;

        private CountingUser(boolean isSuper)
        {
            super("counting_user");
            this.isSuper = isSuper;
        }

        @Override
        public boolean isSuper()
        {
            superuserChecks++;
            return isSuper;
        }

        @Override
        public boolean canLogin()
        {
            return true;
        }
    }
}