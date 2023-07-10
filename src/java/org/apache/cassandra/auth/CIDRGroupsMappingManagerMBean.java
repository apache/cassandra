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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class provides utility functions to list/update/drop CIDR groups mappings.
 * nodetool commands invoke these functions, hence need to register mbean even when
 * CIDR authorization is disabled
 */
public interface CIDRGroupsMappingManagerMBean
{
    /**
     * Retrieves available CIDR groups in the table {@link AuthKeyspace#CIDR_GROUPS}
     * @return returns CIDR groups set
     */
    public Set<String> getAvailableCidrGroups();

    /**
     * Retrieves all CIDRs associated with a given CIDR group in the table {@link AuthKeyspace#CIDR_GROUPS},
     * including invalid CIDRs (if exists)
     * @param cidrGroupName CIDR group to retrieve associated CIDRs
     * @return returns set of CIDRs as strings
     */
    public Set<String> getCidrsOfCidrGroupAsStrings(String cidrGroupName);

    /**
     * Insert/Update CIDR group to CIDRs mapping in the table {@link AuthKeyspace#CIDR_GROUPS}
     * @param cidrGroupName CIDR group name
     * @param cidrs List of CIDRs as strings
     */
    public void updateCidrGroup(String cidrGroupName, List<String> cidrs);

    /**
     * Delete a CIDR group and associated mapping from the table {@link AuthKeyspace#CIDR_GROUPS}
     * throws exception if input cidr group name doesn't exist in the table before deletion
     * @param cidrGroupName CIDR group name
     */
    public void dropCidrGroup(String cidrGroupName);

    /**
     * Update {@link AuthKeyspace#CIDR_GROUPS} table with completly new mappings. Insert nonexisting CIDR groups,
     * update existing CIDR groups with given set of CIDRs and delete nolonger valid CIDR groups.
     * @param cidrGroupsMapping CIDR group mappings
     */
    public void recreateCidrGroupsMapping(Map<String, List<String>> cidrGroupsMapping);

    /**
     * Get CIDR groups of a given IP, when CIDR authorizer is enabled.
     * Throws exception if CIDR authorizer is disabled
     * @param ipStr IP as a string
     * @return Set of strings
     */
    public Set<String> getCidrGroupsOfIP(String ipStr);

    /**
     * Trigger reload of CIDR groups cache
     */
    public void loadCidrGroupsCache();
}
