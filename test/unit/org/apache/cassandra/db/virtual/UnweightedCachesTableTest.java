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

import java.util.Optional;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CIDRPermissionsCache;
import org.apache.cassandra.auth.NetworkPermissionsCacheMBean;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.auth.PermissionsCacheMBean;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.auth.RolesCacheMBean;
import org.apache.cassandra.auth.jmx.AuthorizationProxy;
import org.apache.cassandra.cache.UnweightedCacheSize;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.metrics.UnweightedCacheMetrics;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;

public class UnweightedCachesTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";
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
        table = new TestingUnweightedCachesTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    public void testSelectAllWhenMetricsAreZeroed()
    {
        resetAllCaches();

        assertRows(execute(getSelectQuery()),
                   row(CIDRPermissionsCache.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L),
                   row(PasswordAuthenticator.CredentialsCache.CACHE_NAME, 123, 58, 0L, Double.NaN, 0L, 0L, 0L),
                   row(AuthorizationProxy.JmxPermissionsCache.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L),
                   row(NetworkPermissionsCacheMBean.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L),
                   row(PermissionsCacheMBean.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L),
                   row(RolesCacheMBean.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L));

        table.getAuthenticatorMetrics().ifPresent(p -> {
            p.right.hits.mark(10);
            p.right.requests.mark(40);
        });

        assertRows(execute(getSelectQuery()),
                   row(CIDRPermissionsCache.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L),
                   row(PasswordAuthenticator.CredentialsCache.CACHE_NAME, 123, 58, 10L, 0.25d, 40L, 0L, 0L),
                   row(AuthorizationProxy.JmxPermissionsCache.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L),
                   row(NetworkPermissionsCacheMBean.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L),
                   row(PermissionsCacheMBean.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L),
                   row(RolesCacheMBean.CACHE_NAME, 1000, 0, 0L, Double.NaN, 0L, 0L, 0L));
    }

    @Test
    public void testSelectAllAfterSuperUserOperation() throws Throwable
    {
        resetAllCaches();

        // trigger loading auth caches by a read
        useSuperUser();

        int rows = execute("SELECT name, capacity, entry_count, hit_count, hit_ratio, request_count FROM vts." + table.name()).size();
        assertEquals(6, rows);
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

    private String getSelectQuery()
    {
        return String.format("SELECT %s, %s, %s, %s, %s, %s, %s, %s FROM %s.%s",
                             UnweightedCachesTable.NAME_COLUMN,
                             UnweightedCachesTable.CAPACITY_COLUMN,
                             UnweightedCachesTable.ENTRY_COUNT_COLUMN,
                             UnweightedCachesTable.HIT_COUNT_COLUMN,
                             UnweightedCachesTable.HIT_RATIO_COLUMN,
                             UnweightedCachesTable.REQUEST_COUNT_COLUMN,
                             UnweightedCachesTable.RECENT_REQUEST_RATE_PER_SECOND_COLUMN,
                             UnweightedCachesTable.RECENT_HIT_RATE_PER_SECOND_COLUMN,
                             KS_NAME,
                             table.name());
    }

    private static class TestingUnweightedCachesTable extends UnweightedCachesTable
    {
        private final UnweightedCacheMetrics metrics = new UnweightedCacheMetrics("cache", new UnweightedCacheSize()
        {
            @Override
            public int maxEntries()
            {
                return 123;
            }

            @Override
            public void setMaxEntries(int maxEntries)
            {

            }

            @Override
            public int entries()
            {
                return 58;
            }
        });

        TestingUnweightedCachesTable(String keyspace)
        {
            super(keyspace);
        }

        @Override
        Optional<Pair<String, UnweightedCacheMetrics>> getAuthenticatorMetrics()
        {
            return Optional.of(Pair.create(PasswordAuthenticator.CredentialsCacheMBean.CACHE_NAME, metrics));
        }
    }
}
