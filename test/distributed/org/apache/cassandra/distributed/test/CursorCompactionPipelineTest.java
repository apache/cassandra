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

package org.apache.cassandra.distributed.test;

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs a real compaction on a real node with cursor compaction enabled, and asserts the cursor
 * pipeline is the one that ran.
 *
 * Every other cursor test drives a {@code CompactionTask} it built itself. This one goes through a
 * started node: yaml parsing of {@code cursor_compaction_enabled}, the compaction manager, the
 * strategy, and the executor thread. A silent fallback to the iterator pipeline anywhere in that
 * chain leaves every unit test green, because none of them exercise it.
 *
 * The counters are read inside the instance. They are static state in the node's own classloader, so
 * a read from the test JVM would see zero however the node behaved.
 */
public class CursorCompactionPipelineTest extends TestBaseImpl
{
    private static final int PARTITIONS = 2000;

    @Test
    public void cursorPipelineRunsOnARealNode() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .withConfig(config -> config.set("autocompaction_on_startup_enabled", false)
                                                                         .set("cursor_compaction_enabled", true))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck)) " +
                                              "WITH compaction = {'class':'SizeTieredCompactionStrategy', 'enabled':'false'}"));

            // The node must genuinely have read the yaml setting, not merely accept the config key.
            cluster.get(1).runOnInstance(() ->
                assertTrue("cursor_compaction_enabled did not reach DatabaseDescriptor on the node",
                           DatabaseDescriptor.cursorCompactionEnabled()));

            String padding = "x".repeat(200);
            for (int i = 0; i < PARTITIONS; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?, ?, ?)"),
                                               ConsistencyLevel.ALL, i, 0, padding);
                if (i % 500 == 0)
                    cluster.get(1).flush(KEYSPACE);
            }
            cluster.get(1).flush(KEYSPACE);

            long[] counts = cluster.get(1).callOnInstance(() -> {
                long cursorBefore = org.apache.cassandra.db.compaction.CompactionPipelineCounts.cursorPipelines();
                long iteratorBefore = org.apache.cassandra.db.compaction.CompactionPipelineCounts.iteratorPipelines();

                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").forceMajorCompaction();

                return new long[]{ org.apache.cassandra.db.compaction.CompactionPipelineCounts.cursorPipelines() - cursorBefore,
                                   org.apache.cassandra.db.compaction.CompactionPipelineCounts.iteratorPipelines() - iteratorBefore };
            });

            assertTrue("no cursor pipeline was created for the major compaction, so the node fell back " +
                       "to the iterator path; iterator pipelines created: " + counts[1],
                       counts[0] > 0);
            assertEquals("the node created an iterator pipeline while cursor compaction was enabled",
                         0, counts[1]);

            // The compaction must also have produced correct data, not merely run the right pipeline.
            Object[][] rows = cluster.coordinator(1).execute(
                withKeyspace("SELECT count(*) FROM %s.tbl"), ConsistencyLevel.ALL);
            assertEquals("the cursor compaction lost or duplicated partitions",
                         (long) PARTITIONS, rows[0][0]);
        }
    }
}
