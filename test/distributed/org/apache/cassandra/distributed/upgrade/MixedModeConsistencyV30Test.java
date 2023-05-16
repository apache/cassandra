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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class MixedModeConsistencyV30Test extends UpgradeTestBase
{
    @Test
    public void testConsistency() throws Throwable
    {
        List<Tester> testers = new ArrayList<>();
        testers.addAll(Tester.create(1, ALL));
        testers.addAll(Tester.create(2, ALL, QUORUM));
        testers.addAll(Tester.create(3, ALL, QUORUM, ONE));

        new TestCase()
        .nodes(3)
        .nodesToUpgrade(1)
        .upgradesToCurrentFrom(v30)
        .withConfig(config -> config.set("read_request_timeout_in_ms", SECONDS.toMillis(30))
                                    .set("write_request_timeout_in_ms", SECONDS.toMillis(30)))
        .setup(cluster -> {
            Tester.createTable(cluster);
            for (Tester tester : testers)
                tester.writeRows(cluster);
        }).runAfterNodeUpgrade((cluster, node) -> {
            for (Tester tester : testers)
                tester.readRows(cluster);
        }).run();
    }

    private static class Tester
    {
        private final int numWrittenReplicas;
        private final ConsistencyLevel readConsistencyLevel;
        private final UUID partitionKey;

        private Tester(int numWrittenReplicas, ConsistencyLevel readConsistencyLevel)
        {
            this.numWrittenReplicas = numWrittenReplicas;
            this.readConsistencyLevel = readConsistencyLevel;
            partitionKey = UUID.randomUUID();
        }

        private static List<Tester> create(int numWrittenReplicas, ConsistencyLevel... readConsistencyLevels)
        {
            return Stream.of(readConsistencyLevels)
                         .map(readConsistencyLevel -> new Tester(numWrittenReplicas, readConsistencyLevel))
                         .collect(Collectors.toList());
        }

        private static void createTable(UpgradeableCluster cluster)
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (k uuid, c int, v int, PRIMARY KEY (k, c))"));
        }

        private void writeRows(UpgradeableCluster cluster)
        {
            String query = withKeyspace("INSERT INTO %s.t (k, c, v) VALUES (?, ?, ?)");
            for (int i = 1; i <= numWrittenReplicas; i++)
            {
                IUpgradeableInstance node = cluster.get(i);
                node.executeInternal(query, partitionKey, 1, 10);
                node.executeInternal(query, partitionKey, 2, 20);
                node.executeInternal(query, partitionKey, 3, 30);
            }
        }

        private void readRows(UpgradeableCluster cluster)
        {
            String query = withKeyspace("SELECT * FROM %s.t WHERE k = ?");
            int coordinator = 1;
            try
            {
                for (coordinator = 1; coordinator <= cluster.size(); coordinator++)
                {
                    assertRows(cluster.coordinator(coordinator).execute(query, readConsistencyLevel, partitionKey),
                               row(partitionKey, 1, 10),
                               row(partitionKey, 2, 20),
                               row(partitionKey, 3, 30));
                }
            }
            catch (Throwable t)
            {
                String format = "Unexpected error reading rows with %d written replicas, CL=%s and coordinator=%s";
                throw new AssertionError(format(format, numWrittenReplicas, readConsistencyLevel, coordinator), t);
            }
        }
    }
}
