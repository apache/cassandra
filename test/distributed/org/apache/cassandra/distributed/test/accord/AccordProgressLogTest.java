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

package org.apache.cassandra.distributed.test.accord;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.consensus.TransactionalMode;

public class AccordProgressLogTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(AccordProgressLogTest.class);

    @Test
    public void testRecoveryTimeWindow() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.NETWORK)
                                                             .set("accord.enabled", "true")
                                                             .set("accord.recover_delay", "1s"))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c)) WITH " + TransactionalMode.full.asCqlParam());
            String query = "BEGIN TRANSACTION\n" +
                           "  INSERT INTO ks.tbl (k, c) VALUES (0, 0);\n" +
                           "COMMIT TRANSACTION";

            IMessageFilters.Filter dropPreAccept = cluster.filters().outbound().from(1).to(2).verbs(Verb.ACCORD_PRE_ACCEPT_REQ.id).drop();
            AtomicLong recoveryStartedAt = new AtomicLong();
            Semaphore waitForRecovery = new Semaphore(0);
            IMessageFilters.Filter recovery = cluster.filters().outbound().messagesMatching((from, to, message) -> {
                if (message.verb() == Verb.ACCORD_BEGIN_RECOVER_RSP.id)
                {
                    recoveryStartedAt.compareAndSet(0, System.nanoTime());
                    waitForRecovery.release();
                }
                return false;
            }).drop();

            long coordinationStartedAt = System.nanoTime();
            boolean failed = false;
            try { cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY); }
            catch (Throwable e) { failed = true; }
            Assert.assertTrue(failed);

            waitForRecovery.acquire();
            long timeDeltaMillis = TimeUnit.NANOSECONDS.toMillis(recoveryStartedAt.get() - coordinationStartedAt);
            Assert.assertTrue("Recovery started in " + timeDeltaMillis + "ms", timeDeltaMillis >= 1000);
            Assert.assertTrue("Recovery started in " + timeDeltaMillis + "ms", timeDeltaMillis <= 5000);
        }
    }

    @Test
    public void testFetchTimeWindow() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withoutVNodes()
                                           .withConfig(c -> c.with(Feature.NETWORK).set("accord.enabled", "true"))
                                           .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE ks WITH replication={'class':'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE ks.tbl (k int, c int, v int, primary key (k, c)) WITH " + TransactionalMode.full.asCqlParam());
            String query = "BEGIN TRANSACTION\n" +
                           "  INSERT INTO ks.tbl (k, c) VALUES (0, 0);\n" +
                           "COMMIT TRANSACTION";

            IMessageFilters.Filter dropApply = cluster.filters().outbound().from(1).verbs(Verb.ACCORD_APPLY_REQ.id).drop();
            AtomicLong fetchStartedAt = new AtomicLong();
            Semaphore waitForFetch = new Semaphore(0);
            IMessageFilters.Filter fetch = cluster.filters().outbound().messagesMatching((from, to, message) -> {
                if (message.verb() == Verb.ACCORD_AWAIT_REQ.id)
                {
                    fetchStartedAt.compareAndSet(0, System.nanoTime());
                    waitForFetch.release();
                }
                return false;
            }).drop();

            long coordinationStartedAt = System.nanoTime();
            try
            {
                cluster.coordinator(1).executeWithResult(query, ConsistencyLevel.ANY);
            }
            catch (Throwable e)
            {
            }

            waitForFetch.acquire();
            logger.info("Coordinated at {}", coordinationStartedAt);
            logger.info("Awaited at {}", fetchStartedAt.get());
            long timeDeltaMillis = TimeUnit.NANOSECONDS.toMillis(fetchStartedAt.get() - coordinationStartedAt);
            Assert.assertTrue("Fetch started in " + timeDeltaMillis + "ms", timeDeltaMillis >= 100);
            Assert.assertTrue("Fetch started in " + timeDeltaMillis + "ms", timeDeltaMillis <= 2000);
        }
    }
}
