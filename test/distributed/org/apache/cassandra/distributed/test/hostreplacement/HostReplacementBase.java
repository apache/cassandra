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

package org.apache.cassandra.distributed.test.hostreplacement;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class HostReplacementBase extends TestBaseImpl
{
    static void setupCluster(Cluster cluster)
    {
        fixDistributedSchemas(cluster);
        init(cluster);

        ClusterUtils.awaitGossipSchemaMatch(cluster);

        populate(cluster);
        cluster.forEach(i -> i.flush(KEYSPACE));
    }

    public static void fixDistributedSchemas(Cluster cluster)
    {
        // These keyspaces are under replicated by default, so must be updated when doing a mulit-node cluster;
        // else bootstrap will fail with 'Unable to find sufficient sources for streaming range <range> in keyspace <name>'
        for (String ks : Arrays.asList("system_auth", "system_traces"))
        {
            cluster.schemaChange("ALTER KEYSPACE " + ks + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': " + Math.min(cluster.size(), 3) + "}");
        }

        // in real live repair is needed in this case, but in the test case it doesn't matter if the tables loose
        // anything, so ignoring repair to speed up the tests.
    }

    static void populate(Cluster cluster)
    {
        cluster.schemaChange("CREATE TABLE IF NOT EXISTS " + KEYSPACE + ".tbl (pk int PRIMARY KEY)");
        for (int i = 0; i < 10; i++)
        {
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk) VALUES (?)", ConsistencyLevel.ALL, i);
        }
    }

    static SimpleQueryResult expected()
    {
        QueryResults.Builder builder = QueryResults.builder();
        for (int i = 0; i < 10; i++)
            builder.row(i);
        SimpleQueryResult result = builder.build();
        // sort by pk
        List<Object[]> array = Arrays.asList(result.toObjectArrays());
        Collections.sort(array, Comparator.comparing(HostReplacementBase::getToken));
        return result;
    }

    private static Murmur3Partitioner.LongToken getToken(Object[] row)
    {
        return Murmur3Partitioner.instance.getToken(Int32Type.instance.decompose((Integer) row[0]));
    }

    static void validateRows(ICoordinator coordinator)
    {
        validateRows(coordinator, null);
    }

    static void validateRows(ICoordinator coordinator, ConsistencyLevel level)
    {
        if (level == null) level = ConsistencyLevel.ALL;
        SimpleQueryResult rows = coordinator.executeWithResult("SELECT * FROM " + KEYSPACE + ".tbl", level);
        AssertUtils.assertRows(rows, expected());
    }
}
