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
package org.apache.cassandra.db.monitoring;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InvalidConfiguration extends BadQueryTypes
{
    private static Map<ConsistencyLevel, ConsistencyLevel> incorrectConsistencyLevelMap = new HashMap<>();
    static {
        incorrectConsistencyLevelMap.put(ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM);
        incorrectConsistencyLevelMap.put(ConsistencyLevel.EACH_QUORUM, ConsistencyLevel.LOCAL_QUORUM);
        incorrectConsistencyLevelMap.put(ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL);
        incorrectConsistencyLevelMap.put(ConsistencyLevel.ONE, ConsistencyLevel.LOCAL_ONE);
    }
    String problemText;
    private static final Map<String, Integer> visitedTablesInvalidCompactionType = new ConcurrentHashMap<>();
    private static final Map<String, Integer> visitedTablesInvalidConsistency = new ConcurrentHashMap<>();

    public final static Map<ConsistencyLevel, ConsistencyLevel> INCORRECT_CONSISTENCY_LEVELS = Collections.unmodifiableMap(incorrectConsistencyLevelMap);


    public InvalidConfiguration(String keySpace,
                                String tableName,
                                String problemText)
    {
        super(keySpace, tableName);
        this.problemText = problemText;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(", problemText:");
        sb.append(problemText);
        return sb.toString();
    }

    @Override
    public void cleanup()
    {
        visitedTablesInvalidCompactionType.clear();
        visitedTablesInvalidConsistency.clear();
    }

    @Override
    public String getKey()
    {
        return "";
    }

    @Override
    public String getDetails()
    {
        return problemText;
    }

    static void checkForInvalidCompaction(TableMetadata tableMetadata,
                                          Attributes attrs)
    {
        if ((tableMetadata.params.compaction.klass() == SizeTieredCompactionStrategy.class) && attrs.isTimeToLiveSet())
        {
            if (!visitedTablesInvalidCompactionType.containsKey(tableMetadata.name))
            {
                visitedTablesInvalidCompactionType.put(tableMetadata.name, 0);
                BadQuery.report(BadQuery.BadQueryCategory.INCORRECT_COMPACTION_STRATEBY,
                        new InvalidConfiguration(tableMetadata.keyspace, tableMetadata.name, "found STCS for ttl data, it should have been TWCS for ttl data"));
            }
        }
    }

    static void checkForInvalidConsistency(TableMetadata tableMetadata,
                                           ConsistencyLevel cl)
    {
        Keyspace ks = Schema.instance.getKeyspaceInstance(tableMetadata.keyspace);
        if (ks !=null && ks.getReplicationStrategy().getClass() == NetworkTopologyStrategy.class && INCORRECT_CONSISTENCY_LEVELS.containsKey(cl))
        {
            if (!visitedTablesInvalidConsistency.containsKey(tableMetadata.name))
            {
                visitedTablesInvalidConsistency.put(tableMetadata.name, 0);
                BadQuery.report(BadQuery.BadQueryCategory.INCORRECT_CONSISTENCY_LEVEL,
                        new InvalidConfiguration(tableMetadata.keyspace, tableMetadata.name, String.format("found %s, it should have been %s", cl.name(), INCORRECT_CONSISTENCY_LEVELS.get(cl).name())));
            }
        }
    }
}

