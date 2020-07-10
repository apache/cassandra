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

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.shared.Versions;

public class MixedModeReadTest extends UpgradeTestBase
{
    public static final String SIMPLE_TABLE_NAME = "simple_table";
    public static final String TABLE_WITH_CLUSTERING_NAME = "table_with_clustering";
    public static final String TABLE_WITH_STATIC_NAME = "table_with_static";

    public static final String CREATE_SIMPLE_TABLE = String.format("CREATE TABLE %s.%s (id int, v text, e blob, PRIMARY KEY (id))", KEYSPACE, SIMPLE_TABLE_NAME);
    public static final String CREATE_TABLE_WITH_CLUSTERING = String.format("CREATE TABLE %s.%s (id int, c int, v text, e blob, PRIMARY KEY(id, c))", KEYSPACE, TABLE_WITH_CLUSTERING_NAME);
    public static final String CREATE_TABLE_WITH_STATIC = String.format("CREATE TABLE %s.%s (id int, c int, s int static, v text, e blob, PRIMARY KEY(id, c))", KEYSPACE, TABLE_WITH_STATIC_NAME);

    public static final String INSERT_SIMPLE_TABLE = String.format("INSERT INTO %s.%s (id, v) VALUES (?, ?)", KEYSPACE, SIMPLE_TABLE_NAME);
    public static final String INSERT_TABLE_WITH_CLUSTERING = String.format("INSERT INTO %s.%s (id, c, v) VALUES (?, ?, ?)", KEYSPACE, TABLE_WITH_CLUSTERING_NAME);
    public static final String INSERT_TABLE_WITH_STATIC = String.format("INSERT INTO %s.%s (id, c, s, v) VALUES (?, ?, ?, ?)", KEYSPACE, TABLE_WITH_STATIC_NAME);

    public static final String SELECT_TRACE = "SELECT activity FROM system_traces.events where session_id = ? and source = ? ALLOW FILTERING;";

    @Test
    public void mixedModeReadColumnSubsetDigestCheck() throws Throwable
    {
        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .upgrade(Versions.Major.v30, Versions.Major.v3X)
        .upgrade(Versions.Major.v30, Versions.Major.v4)
        .upgrade(Versions.Major.v3X, Versions.Major.v4)
        .setup(cluster -> {
            cluster.schemaChange(CREATE_SIMPLE_TABLE);
            cluster.schemaChange(CREATE_TABLE_WITH_CLUSTERING);
            cluster.schemaChange(CREATE_TABLE_WITH_STATIC);
            cluster.coordinator(1).execute(INSERT_SIMPLE_TABLE, ConsistencyLevel.ALL, 1, "foo");
            cluster.coordinator(1).execute(INSERT_TABLE_WITH_CLUSTERING, ConsistencyLevel.ALL, 1, 2, "foo");
            cluster.coordinator(1).execute(INSERT_TABLE_WITH_STATIC, ConsistencyLevel.ALL, 1, 2, 3, "foo");

            // baseline to show no digest mismatches before upgrade
            List<String> assertionFailures = Stream.concat(checkTraceForDigestMismatch(cluster, 1),
                                                           checkTraceForDigestMismatch(cluster, 2))
                                                   .collect(Collectors.toList());

            Assert.assertTrue("Assertion failures before upgrading:\n" + StringUtils.join(assertionFailures, "\n"), assertionFailures.isEmpty());
        })
        .runAfterNodeUpgrade((cluster, node) -> {
            if (node != 1)
                return; // shouldn't happen but guard for future test changes

            // should not cause a disgest mismatch in mixed mode
            List<String> assertionFailures = Stream.concat(checkTraceForDigestMismatch(cluster, 1),
                                                           checkTraceForDigestMismatch(cluster, 2))
                                                   .collect(Collectors.toList());

            Assert.assertTrue("Assertion failures after upgrading first node:\n" + StringUtils.join(assertionFailures, "\n"), assertionFailures.isEmpty());
        })
        .run();
    }

    private Stream<String> checkTraceForDigestMismatch(UpgradeableCluster cluster, int coordinatorNode)
    {
        Set<String> projections1 = Sets.newHashSet("id", "v", "e", "id, v", "id, e", "e, v", "id, v, e", "*");
        Set<String> projections2 = Sets.union(projections1, Sets.newHashSet("c", "id, c", "v, c", "e, c", "id, v, c", "id, e, c", "e, v, c", "id, v, e, c"));
        Set<String> projections3 = Sets.union(projections2, Sets.newHashSet("c, s", "id, c, s", "v, c, s", "e, c, s", "id, v, c, s", "id, e, c, s", "e, v, c, s", "id, v, e, c, s"));
        Set<String> projections4 = Sets.newHashSet("id", "s", "id, s");

        Stream<String> queries1 = projections1.stream().map(p -> String.format("SELECT %s FROM %s.%s WHERE id=1", p, KEYSPACE, SIMPLE_TABLE_NAME));
        Stream<String> queries2 = projections2.stream().map(p -> String.format("SELECT %s FROM %s.%s WHERE id=1 AND c=2", p, KEYSPACE, TABLE_WITH_CLUSTERING_NAME));
        Stream<String> queries3 = projections3.stream().map(p -> String.format("SELECT %s FROM %s.%s WHERE id=1 AND c=2", p, KEYSPACE, TABLE_WITH_STATIC_NAME));
        Stream<String> queries4 = projections4.stream().map(p -> String.format("SELECT %s FROM %s.%s WHERE id=1", p, KEYSPACE, TABLE_WITH_STATIC_NAME));

        return Stream.concat(Stream.concat(queries1.flatMap(q -> checkTraceForDigestMismatch(cluster, coordinatorNode, q)),
                                           queries2.flatMap(q -> checkTraceForDigestMismatch(cluster, coordinatorNode, q))),
                             Stream.concat(queries3.flatMap(q -> checkTraceForDigestMismatch(cluster, coordinatorNode, q)),
                                           queries4.flatMap(q -> checkTraceForDigestMismatch(cluster, coordinatorNode, q))));
    }

    private Stream<String> checkTraceForDigestMismatch(UpgradeableCluster cluster, int coordinatorNode, String query)
    {
        Stream.Builder<String> assertionFailures = Stream.builder();
        UUID sessionId = UUID.randomUUID();
        Object[][] results = cluster.coordinator(coordinatorNode).executeWithTracing(sessionId, query, ConsistencyLevel.ALL);
        if (results.length != 1)
            assertionFailures.add(String.format("Query %s returned invalid number of rows on node %d: %d != 1", query, coordinatorNode, results.length));

        results = cluster.coordinator(coordinatorNode)
                         .execute(SELECT_TRACE, ConsistencyLevel.ALL,
                                  sessionId, cluster.get(coordinatorNode).broadcastAddress().getAddress());
        for (Object[] result : results)
        {
            String activity = (String) result[0];
            if (activity.toLowerCase().contains("digest") && activity.toLowerCase().contains("mismatch"))
                assertionFailures.add(String.format("Found Digest Mismatch for query %s when queried from %d node", query, coordinatorNode));
        }

        return assertionFailures.build();
    }
}
