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

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * Encapsulates an intent to repair the given keyspace's tables
 */
public class KeyspaceRepairPlan
{
    private final String keyspaceName;

    private final List<String> tableNames;

    @VisibleForTesting
    public Map<String, Map<Range<Token>, AutoRepairUtils.SizeEstimate>> ksTablesEstimatedBytes;

    public KeyspaceRepairPlan(String keyspaceName, List<String> tableNames, Map<String, Map<Range<Token>, AutoRepairUtils.SizeEstimate>> ksTablesEstimatedBytes)
    {
        this.keyspaceName = keyspaceName;
        this.tableNames = tableNames;
        this.ksTablesEstimatedBytes = ksTablesEstimatedBytes;
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public List<String> getTableNames()
    {
        return tableNames;
    }

    public long getEstimatedBytes()
    {
        return ksTablesEstimatedBytes.values().stream()
                                     .flatMap(tableMap -> tableMap.values().stream())
                                     .mapToLong(sizeEstimate -> sizeEstimate.sizeForRepair)
                                     .sum();
    }

    public long getTableEstimatedBytes(String keyspaceTableName)
    {
        return ksTablesEstimatedBytes.getOrDefault(keyspaceTableName,
                                                   Collections.emptyMap()).values().stream().mapToLong(sizeEstimate -> sizeEstimate.sizeForRepair).sum();
    }

    public AutoRepairUtils.SizeEstimate getSizeEstimate(String keyspaceTableName, Range<Token> tokenRange)
    {
        return ksTablesEstimatedBytes == null ? null
                                              : ksTablesEstimatedBytes.getOrDefault(keyspaceTableName, null) == null ? null
                                                                                                                     : ksTablesEstimatedBytes.get(keyspaceTableName).get(tokenRange);
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        KeyspaceRepairPlan that = (KeyspaceRepairPlan) o;
        return Objects.equals(keyspaceName, that.keyspaceName) && Objects.equals(tableNames, that.tableNames)
               && Objects.equals(ksTablesEstimatedBytes, that.ksTablesEstimatedBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspaceName, tableNames, ksTablesEstimatedBytes);
    }

    @Override
    public String toString()
    {
        return "KeyspaceRepairPlan{" +
               "keyspaceName='" + keyspaceName + '\'' +
               ", tableNames=" + tableNames +
               ", ksTablesEstimatedBytes=" + ksTablesEstimatedBytes +
               '}';
    }
}
