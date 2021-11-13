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
import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.CacheService;

public class CachesTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    @SuppressWarnings("FieldCanBeLocal")
    private CachesTable table;

    @BeforeClass
    public static void setUpClass()
    {
        CQLTester.setUpClass();
        CQLTester.requireAuthentication();
    }

    @Before
    public void config()
    {
        table = new CachesTable(KS_NAME);
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(table)));
    }

    @Test
    public void testSelectAllWhenMetricsAreZeroed() throws Throwable
    {
        resetAllCaches();

        assertRows(execute("SELECT * FROM vts.caches"),
                   row("chunks",         503316480L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("counters",         5242880L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("credentials",         1000L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("jmx_permissions",     1000L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("keys",            11534336L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("network_permissions", 1000L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("permissions",         1000L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("roles",               1000L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L),
                   row("rows",            16777216L, 0, 0L, Double.NaN, 0L, 0L, 0L, 0L));
    }

    private void resetAllCaches()
    {
        ChunkCache.instance.clear();
        ChunkCache.instance.metrics.reset();
        CacheService.instance.counterCache.clear();
        CacheService.instance.counterCache.getMetrics().reset();
        PasswordAuthenticator.CredentialsCache credentialsCache = ((PasswordAuthenticator) DatabaseDescriptor.getAuthenticator()).getCredentialsCache();
        credentialsCache.invalidate();
        credentialsCache.getMetrics().reset();
        AuthorizationProxy.jmxPermissionsCache.invalidate();
        AuthorizationProxy.jmxPermissionsCache.getMetrics().reset();
        CacheService.instance.keyCache.clear();
        CacheService.instance.keyCache.getMetrics().reset();
        AuthenticatedUser.networkPermissionsCache.invalidate();
        AuthenticatedUser.networkPermissionsCache.getMetrics().reset();
        AuthenticatedUser.permissionsCache.invalidate();
        AuthenticatedUser.permissionsCache.getMetrics().reset();
        Roles.cache.invalidate();
        Roles.cache.getMetrics().reset();
        CacheService.instance.rowCache.clear();
        CacheService.instance.rowCache.getMetrics().reset();
    }
}
