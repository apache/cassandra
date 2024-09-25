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


import java.util.List;
import java.util.Objects;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public interface IAutoRepairTokenRangeSplitter
{

    /**
     * Split the token range you wish to repair into multiple assignments.
     * The autorepair framework will repair the list of returned subrange in a sequence.
     * @param repairType The type of repair being executed
     * @param primaryRangeOnly Whether to repair only this node's primary ranges or all of its ranges.
     * @param keyspaceName The keyspace being repaired
     * @param tableNames The tables to repair
     * @return repair assignments broken up by range, keyspace and tables.
     */
    List<RepairAssignment> getRepairAssignments(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, String keyspaceName, List<String> tableNames);

    /**
     * Defines a repair assignment to be issued by the autorepair framework.
     */
    class RepairAssignment
    {
        final Range<Token> tokenRange;

        final String keyspaceName;

        final List<String> tableNames;

        public RepairAssignment(Range<Token> tokenRange, String keyspaceName, List<String> tableNames)
        {
            this.tokenRange = tokenRange;
            this.keyspaceName = keyspaceName;
            this.tableNames = tableNames;
        }

        public Range<Token> getTokenRange()
        {
            return tokenRange;
        }

        public String getKeyspaceName()
        {
            return keyspaceName;
        }

        public List<String> getTableNames()
        {
            return tableNames;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RepairAssignment that = (RepairAssignment) o;
            return Objects.equals(tokenRange, that.tokenRange) && Objects.equals(keyspaceName, that.keyspaceName) && Objects.equals(tableNames, that.tableNames);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tokenRange, keyspaceName, tableNames);
        }

        @Override
        public String toString()
        {
            return "RepairAssignment{" +
                   "tokenRange=" + tokenRange +
                   ", keyspaceName='" + keyspaceName + '\'' +
                   ", tableNames=" + tableNames +
                   '}';
        }
    }
}