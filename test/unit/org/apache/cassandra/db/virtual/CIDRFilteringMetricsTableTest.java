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

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.codahale.metrics.Snapshot;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.auth.AuthCacheService;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.AuthTestUtils;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.metrics.CIDRAuthorizerMetrics;

import static org.apache.cassandra.auth.AuthKeyspace.CIDR_GROUPS;
import static org.apache.cassandra.auth.AuthKeyspace.CIDR_PERMISSIONS;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CIDRFilteringMetricsTableTest extends CQLTester
{
    private static final String KS_NAME = "vts";

    private static void setupSuperUser()
    {
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) " +
                                                     "VALUES ('%s', true, true, '%s')",
                                                     AUTH_KEYSPACE_NAME,
                                                     AuthKeyspace.ROLES,
                                                     CassandraRoleManager.DEFAULT_SUPERUSER_NAME,
                                                     "xxx"));
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();

        SchemaLoader.setupAuth(new AuthTestUtils.LocalCassandraRoleManager(),
                               new AuthTestUtils.LocalPasswordAuthenticator(),
                               new AuthTestUtils.LocalCassandraAuthorizer(),
                               new AuthTestUtils.LocalCassandraNetworkAuthorizer(),
                               new AuthTestUtils.LocalCassandraCIDRAuthorizer());
        AuthCacheService.initializeAndRegisterCaches();
        setupSuperUser();
    }

    @Before
    public void clear()
    {
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(CIDR_GROUPS).truncateBlocking();
        Keyspace.open(AUTH_KEYSPACE_NAME).getColumnFamilyStore(CIDR_PERMISSIONS).truncateBlocking();
    }

    private void queryAndValidateCountMetrics() throws Throwable
    {
        CIDRAuthorizerMetrics cidrAuthorizerMetrics = DatabaseDescriptor.getCIDRAuthorizer().getCidrAuthorizerMetrics();

        String getMetricsQuery = "SELECT * FROM " + KS_NAME + '.' +
                                 CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.TABLE_NAME;
        UntypedResultSet vtsRows = execute(getMetricsQuery);

        assertEquals(5, vtsRows.size());

        Iterator<UntypedResultSet.Row> it = vtsRows.iterator();
        while (it.hasNext())
        {
            UntypedResultSet.Row row = it.next();
            String metricName = row.getString(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.NAME_COL);
            long metricValue = row.getLong(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.VALUE_COL);
            assertTrue(metricValue != 0);

            if (CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.CIDR_GROUPS_CACHE_RELOAD_COUNT_NAME
                .equals(metricName))
                assertEquals(cidrAuthorizerMetrics.cacheReloadCount.getCount(), metricValue, 0);
            else
            {
                if (metricName.contains(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable
                                        .CIDR_ACCESSES_REJECTED_COUNT_NAME_PREFIX))
                    assertEquals(cidrAuthorizerMetrics.rejectedCidrAccessCount.get(metricName.split(
                        CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable
                        .CIDR_ACCESSES_REJECTED_COUNT_NAME_PREFIX)[1]).getCount(), metricValue, 0);
                else
                    assertEquals(cidrAuthorizerMetrics.acceptedCidrAccessCount.get(metricName.split(
                        CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable
                        .CIDR_ACCESSES_ACCEPTED_COUNT_NAME_PREFIX)[1]).getCount(), metricValue, 0);
            }
        }
    }

    private void verifyLatencies(Snapshot snapshot, UntypedResultSet.Row row)
    {
        assertEquals(snapshot.getMedian(),
                     row.getDouble(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.P50_COL), 0.01);
        assertEquals(snapshot.get95thPercentile(),
                     row.getDouble(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.P95_COL), 0.01);
        assertEquals(snapshot.get99thPercentile(),
                     row.getDouble(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.P99_COL), 0.01);
        assertEquals(snapshot.get999thPercentile(),
                     row.getDouble(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.P999_COL), 0.01);
        assertEquals(snapshot.getMax(),
                     row.getDouble(CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.MAX_COL), 0.01);
    }

    private void queryAndValidateLatencyMetrics() throws Throwable
    {
        CIDRAuthorizerMetrics cidrAuthorizerMetrics = DatabaseDescriptor.getCIDRAuthorizer().getCidrAuthorizerMetrics();

        String getMetricsQuery = "SELECT * FROM " + KS_NAME + '.' +
                                 CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.TABLE_NAME;
        UntypedResultSet vtsRows = execute(getMetricsQuery);

        assertEquals(3, vtsRows.size());

        Iterator<UntypedResultSet.Row> it = vtsRows.iterator();
        while (it.hasNext())
        {
            UntypedResultSet.Row row = it.next();
            String metricName = row.getString(CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable.NAME_COL);

            switch (metricName)
            {
                case CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.CIDR_CHECKS_LATENCY_NAME:
                    verifyLatencies(cidrAuthorizerMetrics.cidrChecksLatency.getSnapshot(), row);
                    break;
                case CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.CIDR_GROUPS_CACHE_RELOAD_LATENCY_NAME:
                    verifyLatencies(cidrAuthorizerMetrics.cacheReloadLatency.getSnapshot(), row);
                    break;
                case CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable.LOOKUP_CIDR_GROUPS_FOR_IP_LATENCY_NAME:
                    verifyLatencies(cidrAuthorizerMetrics.lookupCidrGroupsForIpLatency.getSnapshot(), row);
                    break;
            }
        }
    }

    @Test
    public void testCidrFilteringStats() throws Throwable
    {
        CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable countsTable =
        new CIDRFilteringMetricsTable.CIDRFilteringMetricsCountsTable(KS_NAME);

        CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable latenciesTable =
        new CIDRFilteringMetricsTable.CIDRFilteringMetricsLatenciesTable(KS_NAME);

        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS_NAME, ImmutableList.of(countsTable,
                                                                                                latenciesTable)));

        Map<String, List<String>> usersList = new HashMap<String, List<String>>()
        {{
            put("user1", Collections.singletonList("cidrGroup1"));
            put("user2", Arrays.asList("cidrGroup2", "cidrGroup3"));
        }};

        Map<String, List<CIDR>> cidrsMapping = new HashMap<String, List<CIDR>>()
        {{
            put("cidrGroup1", Collections.singletonList(CIDR.getInstance("10.20.30.5/24")));
            put("cidrGroup2", Arrays.asList(CIDR.getInstance("20.30.40.6/16"), CIDR.getInstance("30.40.50.6/8")));
            put("cidrGroup3", Arrays.asList(CIDR.getInstance("40.50.60.7/32"), CIDR.getInstance("50.60.70.80/10"),
                                            CIDR.getInstance("60.70.80.90/22")));
        }};

        AuthTestUtils.createUsersWithCidrAccess(usersList);
        AuthTestUtils.insertCidrsMappings(cidrsMapping);

        AuthenticatedUser user = new AuthenticatedUser("user1");
        assertTrue(user.hasAccessFromIp(new InetSocketAddress("10.20.30.5", 0)));
        Assert.assertFalse(user.hasAccessFromIp(new InetSocketAddress("11.20.30.5", 0)));
        Assert.assertFalse(user.hasAccessFromIp(new InetSocketAddress("20.30.140.60", 0)));
        Assert.assertFalse(user.hasAccessFromIp(new InetSocketAddress("50.60.170.180", 0)));

        queryAndValidateCountMetrics();
        queryAndValidateLatencyMetrics();
    }

}
