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

package org.apache.cassandra.db.virtual;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

public class UnweightedCachesTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @SuppressWarnings("FieldCanBeLocal")
    private UnweightedCachesTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
    }

    @Before
    public void config()
    {
        table = new UnweightedCachesTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    public void testSelectAllWhenMetricsAreZeroed() throws Throwable
    {
        resetAllCaches();

        // We cannot compare "rate" metrics since they are dynamic, we can end up with having a flaky test
        assertRows(execute("SELECT " +
                           "name, capacity, entry_count, hit_count, hit_ratio, request_count " +
                           "FROM vts.unweighted_caches"),
                   row("credentials", 1000L, 0, 0L, Double.NaN, 0L),
                   row("jmx_permissions", 1000L, 0, 0L, Double.NaN, 0L),
                   row("network_permissions", 1000L, 0, 0L, Double.NaN, 0L),
                   row("permissions", 1000L, 0, 0L, Double.NaN, 0L),
                   row("roles", 1000L, 0, 0L, Double.NaN, 0L));
    }

    @Test
    public void testSelectAllAfterSuperUserOperation() throws Throwable
    {
        resetAllCaches();

        // trigger loading auth caches by a read
        useSuperUser();
        executeNet("SELECT name, entry_count FROM vts.unweighted_caches");

        // We cannot compare "rate" metrics since they are dynamic, we can end up with having a flaky test;
        // Comparing request_count and hit_count does not seem to be straightforward and relies on internal
        // implementation which can be changed, so we do not do that as well;
        // We check entry_count only which seems to be a good choice and enough to ensure that basic functionality
        // is covered by tests.
        assertRows(execute("SELECT name, entry_count FROM vts.unweighted_caches"),
                   row("credentials", 1),
                   // jmx permissins cache is not affected by the above read operation
                   row("jmx_permissions", 0),
                   row("network_permissions", 1),
                   row("permissions", 1),
                   row("roles", 1));
    }

    private void resetAllCaches()
    {
        PasswordAuthenticator.CredentialsCache credentialsCache = ((PasswordAuthenticator) DatabaseDescriptor.getAuthenticator()).getCredentialsCache();
        credentialsCache.invalidate();
        credentialsCache.getMetrics().reset();
        AuthorizationProxy.jmxPermissionsCache.invalidate();
        AuthorizationProxy.jmxPermissionsCache.getMetrics().reset();
        AuthenticatedUser.networkPermissionsCache.invalidate();
        AuthenticatedUser.networkPermissionsCache.getMetrics().reset();
        AuthenticatedUser.permissionsCache.invalidate();
        AuthenticatedUser.permissionsCache.getMetrics().reset();
        Roles.cache.invalidate();
        Roles.cache.getMetrics().reset();
    }
}
