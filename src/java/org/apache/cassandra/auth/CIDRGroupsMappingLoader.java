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

import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.CIDR;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.Pair;

/**
 * Reads the table {@link AuthKeyspace#CIDR_GROUPS} and populates CIDR groups mapping data structures.
 * Maintains separate mappings for IPv4 and IPv6 CIDRs
 */
public class CIDRGroupsMappingLoader
{
    private static final Logger logger = LoggerFactory.getLogger(CIDRGroupsMappingLoader.class);

    CIDRGroupsMappingTable.Builder<String> ipv4CidrGroupsTableBuilder = CIDRGroupsMappingTable.builder(false);
    CIDRGroupsMappingTable.Builder<String> ipv6CidrGroupsTableBuilder = CIDRGroupsMappingTable.builder(true);

    // Maintain separate mappings for IPv4 and IPv6 CIDRs
    private CIDRGroupsMappingTable<String> ipv4CidrGroupsTable = null;
    private CIDRGroupsMappingTable<String> ipv6CidrGroupsTable = null;

    protected CIDRGroupsMappingManager cidrGroupsMappingManager;

    public CIDRGroupsMappingLoader(CIDRGroupsMappingManager cidrGroupsMappingManager)
    {
        this.cidrGroupsMappingManager = cidrGroupsMappingManager;

        populateCidrGroupsMapping();
    }

    private void populateCidrGroupsMapping()
    {
        UntypedResultSet rows = cidrGroupsMappingManager.getCidrGroupsTableEntries();
        for (UntypedResultSet.Row row : rows)
        {
            String cidrGroupName = row.getString(AuthKeyspace.CIDR_GROUPS_TBL_CIDR_GROUP_COL_NAME);
            Set<Pair<InetAddress, Short>> cidrs = cidrGroupsMappingManager.retrieveCidrsFromRow(row);

            for (Pair<InetAddress, Short> cidr : cidrs)
            {
                try
                {
                    CIDR validCidr = new CIDR(cidr.left(), cidr.right);
                    if (validCidr.isIPv6())
                        ipv6CidrGroupsTableBuilder.add(validCidr, cidrGroupName);
                    else
                        ipv4CidrGroupsTableBuilder.add(validCidr, cidrGroupName);
                }
                catch (RuntimeException e)
                {
                    // We should not have this scenario after introducing CIDR cql datatype which ensures
                    // invalid CIDRs can't be inserted into the table using insert/update cql commands
                    logger.error("Invalid CIDR {} found in the table, ignoring this entry", cidr);
                }
            }
        }

        ipv4CidrGroupsTable = ipv4CidrGroupsTableBuilder.build();
        ipv6CidrGroupsTable = ipv6CidrGroupsTableBuilder.build();
    }

    public Set<String> lookupCidrGroupsCacheForIp(InetAddress ipAddr)
    {
        // Based on the incoming IP format, lookup in appropriate CIDR groups mapping
        if (ipAddr instanceof Inet6Address)
            return ipv6CidrGroupsTable.lookupLongestMatchForIP(ipAddr);
        else
            return ipv4CidrGroupsTable.lookupLongestMatchForIP(ipAddr);
    }
}
