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

package org.apache.cassandra.repair.autorepair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Immutable snapshot of a keyspace's repair ranges and estimates, shared by statistics and assignment generation.
 */
public class KeyspaceRepairPlan
{
    private final String keyspaceName;

    private final List<String> tableNames;

    private final List<Range<Token>> tokenRanges;

    private final Map<String, Map<Range<Token>, AutoRepairUtils.SizeEstimate>> ksTablesEstimatedBytes;

    public KeyspaceRepairPlan(String keyspaceName, List<String> tableNames, List<Range<Token>> tokenRanges,
                              Map<String, Map<Range<Token>, AutoRepairUtils.SizeEstimate>> ksTablesEstimatedBytes)
    {
        this.keyspaceName = keyspaceName;
        this.tableNames = ImmutableList.copyOf(tableNames);
        this.tokenRanges = ImmutableList.copyOf(tokenRanges);
        Set<Range<Token>> ranges = ImmutableSet.copyOf(this.tokenRanges);
        checkArgument(ranges.size() == this.tokenRanges.size(), "Duplicate repair ranges for %s", keyspaceName);
        checkArgument(ksTablesEstimatedBytes.size() == this.tableNames.size(), "Size estimates must match the planned tables for %s", keyspaceName);
        ImmutableMap.Builder<String, Map<Range<Token>, AutoRepairUtils.SizeEstimate>> estimates = ImmutableMap.builder();
        for (String tableName : this.tableNames)
        {
            String keyspaceTable = AutoRepairUtils.getKeyspaceTableName(keyspaceName, tableName);
            Map<Range<Token>, AutoRepairUtils.SizeEstimate> tableEstimates = ksTablesEstimatedBytes.get(keyspaceTable);
            checkArgument(tableEstimates != null && tableEstimates.keySet().equals(ranges),
                          "Size estimates for %s must match the planned ranges", keyspaceTable);
            estimates.put(keyspaceTable, ImmutableMap.copyOf(tableEstimates));
        }
        this.ksTablesEstimatedBytes = estimates.build();
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public List<String> getTableNames()
    {
        return tableNames;
    }

    public List<Range<Token>> getTokenRanges()
    {
        return tokenRanges;
    }

    public long getEstimatedBytes()
    {
        return ksTablesEstimatedBytes.values().stream()
                                     .flatMap(tableMap -> tableMap.values().stream())
                                     .mapToLong(AutoRepairUtils.SizeEstimate::getEstimatedBytes)
                                     .sum();
    }

    public long getTableEstimatedBytes(String keyspaceTableName)
    {
        return ksTablesEstimatedBytes.getOrDefault(keyspaceTableName,
                                                   Collections.emptyMap()).values().stream().mapToLong(AutoRepairUtils.SizeEstimate::getEstimatedBytes).sum();
    }

    public AutoRepairUtils.SizeEstimate getSizeEstimate(String keyspaceTableName, Range<Token> tokenRange)
    {
        return ksTablesEstimatedBytes.getOrDefault(keyspaceTableName, Collections.emptyMap()).get(tokenRange);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        KeyspaceRepairPlan that = (KeyspaceRepairPlan) o;
        return Objects.equals(keyspaceName, that.keyspaceName) && Objects.equals(tableNames, that.tableNames)
               && Objects.equals(tokenRanges, that.tokenRanges)
               && Objects.equals(ksTablesEstimatedBytes, that.ksTablesEstimatedBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspaceName, tableNames, tokenRanges, ksTablesEstimatedBytes);
    }

    @Override
    public String toString()
    {
        return "KeyspaceRepairPlan{" +
               "keyspaceName='" + keyspaceName + '\'' +
               ", tableNames=" + tableNames +
               ", tokenRanges=" + tokenRanges +
               ", ksTablesEstimatedBytes=" + ksTablesEstimatedBytes +
               '}';
    }
}
