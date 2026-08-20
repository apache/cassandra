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

import java.util.Set;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CIDRPermissionsTest
{
    private static boolean canAccessFrom(Set<String> permissions, Set<String> cidrGroups)
    {
        return CIDRPermissions.subset(permissions).canAccessFrom(cidrGroups);
    }

    @Test
    public void testSameSizeSubset()
    {
        assertThat(canAccessFrom(Set.of("cidrGroup1"), Set.of("cidrGroup1"))).isTrue();
        assertThat(canAccessFrom(Set.of("cidrGroup1"), Set.of("cidrGroup2"))).isFalse();

        assertThat(canAccessFrom(Set.of("cidrGroup1", "cidrGroup2"), Set.of("cidrGroup2", "cidrGroup3"))).isTrue();
        assertThat(canAccessFrom(Set.of("cidrGroup1", "cidrGroup2"), Set.of("cidrGroup3", "cidrGroup4"))).isFalse();
    }

    @Test
    public void testSubsetSmallerThanCidrGroups()
    {
        // an IP resolves to several CIDR groups when the same CIDR is mapped to more than one of them
        Set<String> cidrGroups = Set.of("cidrGroup1", "cidrGroup2", "cidrGroup3");

        assertThat(canAccessFrom(Set.of("cidrGroup1"), cidrGroups)).isTrue();
        assertThat(canAccessFrom(Set.of("cidrGroup3"), cidrGroups)).isTrue();
        assertThat(canAccessFrom(Set.of("cidrGroup4"), cidrGroups)).isFalse();

        assertThat(canAccessFrom(Set.of("cidrGroup1", "cidrGroup4"), cidrGroups)).isTrue();
        assertThat(canAccessFrom(Set.of("cidrGroup4", "cidrGroup5"), cidrGroups)).isFalse();
    }

    @Test
    public void testSubsetLargerThanCidrGroups()
    {
        Set<String> permissions = Set.of("cidrGroup1", "cidrGroup2", "cidrGroup3");

        assertThat(canAccessFrom(permissions, Set.of("cidrGroup1"))).isTrue();
        assertThat(canAccessFrom(permissions, Set.of("cidrGroup3"))).isTrue();
        assertThat(canAccessFrom(permissions, Set.of("cidrGroup4"))).isFalse();

        assertThat(canAccessFrom(permissions, Set.of("cidrGroup3", "cidrGroup4"))).isTrue();
        assertThat(canAccessFrom(permissions, Set.of("cidrGroup4", "cidrGroup5"))).isFalse();
    }

    @Test
    public void testEmptySets()
    {
        // no CIDR group matched the IP
        assertThat(canAccessFrom(Set.of("cidrGroup1"), Set.of())).isFalse();
        assertThat(canAccessFrom(Set.of("cidrGroup1", "cidrGroup2"), Set.of())).isFalse();

        // a role without CIDR groups is turned into CIDRPermissions.all() by CIDRPermissionsManager,
        // an explicitly empty subset allows nothing
        assertThat(canAccessFrom(Set.of(), Set.of("cidrGroup1"))).isFalse();
        assertThat(canAccessFrom(Set.of(), Set.of())).isFalse();
    }

    @Test
    public void testSubsetProperties()
    {
        CIDRPermissions permissions = CIDRPermissions.subset(Set.of("cidrGroup1", "cidrGroup2"));

        assertThat(permissions.restrictsAccess()).isTrue();
        assertThat(permissions.allowedCIDRGroups()).containsExactlyInAnyOrder("cidrGroup1", "cidrGroup2");
    }

    @Test
    public void testAllAndNone()
    {
        assertThat(CIDRPermissions.all().canAccessFrom(Set.of("cidrGroup1"))).isTrue();
        assertThat(CIDRPermissions.all().canAccessFrom(Set.of())).isTrue();
        assertThat(CIDRPermissions.all().restrictsAccess()).isFalse();

        assertThat(CIDRPermissions.none().canAccessFrom(Set.of("cidrGroup1"))).isFalse();
        assertThat(CIDRPermissions.none().canAccessFrom(Set.of())).isFalse();
        assertThat(CIDRPermissions.none().restrictsAccess()).isTrue();
    }
}
