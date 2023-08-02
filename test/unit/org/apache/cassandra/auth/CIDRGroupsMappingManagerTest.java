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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

public class CIDRGroupsMappingManagerTest
{
    CIDRGroupsMappingManager cidrGroupsMappingManager;

    private static void setupSuperUser()
    {
        QueryProcessor.executeInternal(String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) "
                                                     + "VALUES ('%s', true, true, '%s')",
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
    public void setup()
    {
        cidrGroupsMappingManager = new AuthTestUtils.LocalCIDRGroupsMappingManager();
        cidrGroupsMappingManager.setup();

        Map<String, List<CIDR>> cidrsMapping = new HashMap<String, List<CIDR>>()
        {{
            put("cidrGroup1", Collections.singletonList(CIDR.getInstance("10.20.30.5/24")));
            put("cidrGroup2", Arrays.asList(CIDR.getInstance("20.30.40.6/16"), CIDR.getInstance("30.40.50.6/8")));
            put("cidrGroup3", Arrays.asList(CIDR.getInstance("40.50.60.7/32"), CIDR.getInstance("50.60.70.80/10"), CIDR.getInstance("60.70.80.90/22")));
        }};

        AuthTestUtils.insertCidrsMappings(cidrsMapping);
    }

    @Test
    public void testGetCidrsOfCidrGroupAsStrings()
    {
        Set<String> cidrs = cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup2");
        assertEquals(ImmutableSet.of("20.30.0.0/16", "30.0.0.0/8"), cidrs);
    }

    @Test
    public void testUpdateCidrGroup()
    {
        cidrGroupsMappingManager.updateCidrGroup("cidrGroup3",
                                                 Arrays.asList("140.50.60.7/32", "150.60.70.80/10"));
        assertEquals(ImmutableSet.of("150.0.0.0/10", "140.50.60.7/32"),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup3"));
    }

    @Test
    public void testDropCidrGroupForExistingCidrGroup()
    {
        cidrGroupsMappingManager.dropCidrGroup("cidrGroup1");
        assertEquals(2, cidrGroupsMappingManager.getAvailableCidrGroups().size());
    }

    @Test
    public void testDropCidrGroupForNonExistingCidrGroup()
    {
        assertThatThrownBy(() -> cidrGroupsMappingManager.dropCidrGroup("cidrGroup10"))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("CIDR group 'cidrGroup10' doesn't exists");
    }

    @Test
    public void testDropCidrGroupIfExistsForExistingCidrGroup()
    {
        cidrGroupsMappingManager.dropCidrGroupIfExists("cidrGroup1");
        assertEquals(2, cidrGroupsMappingManager.getAvailableCidrGroups().size());
    }

    @Test
    public void testDropCidrGroupIfExistsForNonExistingCidrGroup()
    {
        // expect no exception
        cidrGroupsMappingManager.dropCidrGroupIfExists("cidrGroup10");
    }

    @Test
    public void testRecreateCidrGroupsMapping()
    {
        assertEquals(3, cidrGroupsMappingManager.getAvailableCidrGroups().size());
        assertEquals(ImmutableSet.of("10.20.30.0/24"),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup1"));
        assertEquals(ImmutableSet.of("20.30.0.0/16", "30.0.0.0/8"),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup2"));
        assertEquals(ImmutableSet.of("40.50.60.7/32", "50.0.0.0/10", "60.70.80.0/22"),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup3"));

        // load table with new mappings
        Map<String, List<String>> cidrsMapping = new HashMap<String, List<String>>()
        {{
            put("cidrGroup11", Collections.singletonList("100.20.30.5/24"));
            put("cidrGroup12", Arrays.asList("120.30.40.6/16", "130.40.50.6/8"));
        }};
        cidrGroupsMappingManager.recreateCidrGroupsMapping(cidrsMapping);

        assertEquals(2, cidrGroupsMappingManager.getAvailableCidrGroups().size());
        assertEquals(ImmutableSet.of("100.20.30.0/24"),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup11"));
        assertEquals(ImmutableSet.of("120.30.0.0/16", "130.0.0.0/8"),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup12"));

        // ensure stale cidr groups are deleted
        assertEquals(Collections.emptySet(),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup1"));
        assertEquals(Collections.emptySet(),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup2"));
        assertEquals(Collections.emptySet(),
                     cidrGroupsMappingManager.getCidrsOfCidrGroupAsStrings("cidrGroup3"));
    }
}