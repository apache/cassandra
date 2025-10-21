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

package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NoTombstonePurgingTest extends TestBaseImpl
{
    @Test
    public void testNp() throws IOException
    {
        testHelper((cluster) -> {
            // full repair, with -np, tombstone gets streamed
            cluster.get(1).nodetoolResult("repair", "--include-gcgs-expired-tombstones", "--full", KEYSPACE, "tbl");
        });
    }

    private void testHelper(Consumer<Cluster> repair) throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.set("hinted_handoff_enabled", false)
                                                             .with(Feature.values()))
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key) with gc_grace_seconds = 1 and compaction={'class':'SizeTieredCompactionStrategy', 'enabled':false}"));
            cluster.get(1).executeInternal(withKeyspace("delete from %s.tbl where id = 5"));
            cluster.get(1).flush(KEYSPACE);
            Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS); //gcgs expiry

            // incremental repair, the tombstone is purgeable, will not get included in MT calculation
            cluster.get(1).nodetoolResult("repair", KEYSPACE, "tbl");
            cluster.get(2).runOnInstance(() -> assertTrue(Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().isEmpty()));

            // full repair, tombstone still gets purged
            cluster.get(1).nodetoolResult("repair", "--full", KEYSPACE, "tbl");
            cluster.get(2).runOnInstance(() -> assertTrue(Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().isEmpty()));

            repair.accept(cluster);

            cluster.get(2).runOnInstance(() -> assertFalse(Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().isEmpty()));
        }
    }
}
