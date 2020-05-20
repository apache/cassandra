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

package org.apache.cassandra.distributed.upgrade;

import java.util.Arrays;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.DistributedTestBase;
import org.apache.cassandra.distributed.shared.Versions;

import static org.junit.Assert.fail;

public class MixedModeReadRepairTest extends UpgradeTestBase
{
    @Test
    public void mixedModeReadRepairCompactStorage() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup((cluster) -> cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk ascii, b boolean, v blob, PRIMARY KEY (pk)) WITH COMPACT STORAGE"))
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node != 1)
                return;
            // now node1 is 3.0 and node2 is 2.2
            // make sure 2.2 side does not get the mutation
            cluster.get(1).executeInternal("DELETE FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                                                          "something");
            // trigger a read repair
            cluster.coordinator(2).execute("SELECT * FROM " + DistributedTestBase.KEYSPACE + ".tbl WHERE pk = ?",
                                           ConsistencyLevel.ALL,
                                           "something");
            cluster.get(2).flush(DistributedTestBase.KEYSPACE);
        })
        .runAfterClusterUpgrade((cluster) -> cluster.get(2).forceCompact(DistributedTestBase.KEYSPACE, "tbl"))
        .run();
    }

    @Test
    public void mixedModeReadRepairDuplicateRows() throws Throwable
    {
        final String[] workload1 = new String[]
        {
            "DELETE FROM " + DistributedTestBase.KEYSPACE + ".tbl USING TIMESTAMP 1 WHERE pk = 1 AND ck = 2;",
            "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, {'a':'b'}) USING TIMESTAMP 3;",
            "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, {'c':'d'}) USING TIMESTAMP 3;",
            "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 3, {'e':'f'}) USING TIMESTAMP 3;",
        };

        final String[] workload2 = new String[]
        {
            "INSERT INTO " + DistributedTestBase.KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 2, {'g':'h'}) USING TIMESTAMP 5;",
        };

        new TestCase()
        .nodes(2)
        .upgrade(Versions.Major.v22, Versions.Major.v30)
        .setup((cluster) ->
        {
            cluster.schemaChange("CREATE TABLE " + DistributedTestBase.KEYSPACE + ".tbl (pk int, ck int, v map<text, text>, PRIMARY KEY (pk, ck));");
        })
        .runAfterNodeUpgrade((cluster, node) ->
        {
            if (node == 2)
                return;

            // now node1 is 3.0 and node2 is 2.2
            for (int i = 0; i < workload1.length; i++ )
                cluster.coordinator(2).execute(workload1[i], ConsistencyLevel.QUORUM);

            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).flush(KEYSPACE);

            validate(cluster, 2, false);

            for (int i = 0; i < workload2.length; i++ )
                cluster.coordinator(2).execute(workload2[i], ConsistencyLevel.QUORUM);

            cluster.get(1).flush(KEYSPACE);
            cluster.get(2).flush(KEYSPACE);

            validate(cluster, 1, true);
        })
        .run();
    }

    private void validate(UpgradeableCluster cluster, int nodeid, boolean local)
    {
        String query = "SELECT * FROM " + KEYSPACE + ".tbl";

        Iterator<Object[]> iter = local
                                ? Iterators.forArray(cluster.get(nodeid).executeInternal(query))
                                : cluster.coordinator(nodeid).executeWithPaging(query, ConsistencyLevel.ALL, 2);

        Object[] prevRow = null;
        Object prevClustering = null;

        while (iter.hasNext())
        {
            Object[] row = iter.next();
            Object clustering = row[1];

            if (clustering.equals(prevClustering))
            {
                fail(String.format("Duplicate rows on node %d in %s mode: \n%s\n%s",
                                   nodeid,
                                   local ? "local" : "distributed",
                                   Arrays.toString(prevRow),
                                   Arrays.toString(row)));
            }

            prevRow = row;
            prevClustering = clustering;
        }
    }
}
