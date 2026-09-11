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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.distributed.test;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.junit.Assert.assertEquals;

public class CompactionHistoryShutdownTest extends TestBaseImpl
{
    @Test
    public void testHistoryWriterShutdownWithOnExitThreads() throws Exception
    {
        try (Cluster cluster = Cluster.build(1).start())
        {
            cluster.get(1).runOnInstance(() -> {
                TimeUUID id = TimeUUID.Generator.nextTimeUUID();
                FBUtilities.waitOnFuture(SystemKeyspace.updateCompactionHistory(id, "history_shutdown_ks", "history_shutdown_cf",
                                                                               System.currentTimeMillis(), 1000, 500,
                                                                               Collections.singletonMap(1, 100L),
                                                                               Collections.singletonMap("strategy", "STCS")),
                                         Duration.ofSeconds(10));
                assertEquals(1, executeInternal("SELECT id FROM system.compaction_history WHERE id=?", id).size());
            });

            // Successful history insertion starts the persistent worker. Instance.shutdown must stop
            // it independently of StorageService.drain and pass the harness's thread-leak assertion.
            cluster.get(1).shutdown(true).get(2, TimeUnit.MINUTES);
        }
    }
}
