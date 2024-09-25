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


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.service.AutoRepairService;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.repair.autorepair.AutoRepairUtils.splitEvenly;

public class DefaultAutoRepairTokenSplitter implements IAutoRepairTokenRangeSplitter
{
    @Override
    public List<RepairAssignment> getRepairAssignments(AutoRepairConfig.RepairType repairType, boolean primaryRangeOnly, String keyspaceName, List<String> tableNames)
    {
        AutoRepairConfig config = AutoRepairService.instance.getAutoRepairConfig();
        List<RepairAssignment> repairAssignments = new ArrayList<>();

        Collection<Range<Token>> tokens = StorageService.instance.getPrimaryRanges(keyspaceName);
        if (!primaryRangeOnly)
        {
            // if we need to repair non-primary token ranges, then change the tokens accrodingly
            tokens = StorageService.instance.getLocalReplicas(keyspaceName).ranges();
        }
        int numberOfSubranges = config.getRepairSubRangeNum(repairType);

        boolean byKeyspace = config.getRepairByKeyspace(repairType);

        // collect all token ranges.
        List<Range<Token>> allRanges = new ArrayList<>();
        for (Range<Token> token : tokens)
        {
            allRanges.addAll(splitEvenly(token, numberOfSubranges));
        }

        if (byKeyspace)
        {
            for (Range<Token> splitRange : allRanges)
            {
                // add repair assignment for each range entire keyspace's tables
                repairAssignments.add(new RepairAssignment(splitRange, keyspaceName, tableNames));
            }
        }
        else
        {
            // add repair assignment per table
            for (String tableName : tableNames)
            {
                for (Range<Token> splitRange : allRanges)
                {
                    repairAssignments.add(new RepairAssignment(splitRange, keyspaceName, Collections.singletonList(tableName)));
                }
            }
        }
        return repairAssignments;
    }
}