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

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ProfileLoadTest extends TestBaseImpl
{
    @Test
    public void testScheduledSamplingTaskLogs() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            // start the scheduled profileload task that samples for 1 second and every second.
            cluster.get(1).nodetoolResult("profileload", "1000", "-i", "1000").asserts().success();

            Random rnd = new Random();
            // 800 * 2ms = 1.6 seconds. It logs every second. So it logs at least once.
            for (int i = 0; i < 800; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?,?,?)"),
                                ConsistencyLevel.QUORUM, rnd.nextInt(), rnd.nextInt(), i);
                Uninterruptibles.sleepUninterruptibly(2, TimeUnit.MILLISECONDS);
            }
            // --list should display all active tasks.
            String expectedOutput = String.format("KEYSPACE TABLE%n" + "%8s %5s", "*", "*");
            cluster.get(1).nodetoolResult("profileload", "--list")
                   .asserts()
                   .success()
                   .stdoutContains(expectedOutput);

            // loop assert the log contains the frequency readout; give this 15 seconds which should be plenty of time
            // even on very badly underprovisioned environments
            int timeout = 15;
            boolean testPassed = false;
            while (timeout-- > 0)
            {
                List<String> freqHeadings = cluster.get(1)
                                                   .logs()
                                                   .grep("Frequency of (reads|writes|cas contentions) by partition")
                                                   .getResult();
                if (freqHeadings.size() > 3)
                {
                    testPassed = true;
                    break;
                }
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            Assert.assertTrue("The scheduled task should at least run and log once", testPassed);

            List<String> startSamplingLogs = cluster.get(1)
                                                    .logs()
                                                    .grep("Starting to sample tables")
                                                    .getResult();
            Assert.assertTrue("It should start sampling at least once", startSamplingLogs.size() > 0);

            // stop the scheduled sampling
            cluster.get(1).nodetoolResult("profileload", "--stop").asserts().success();

            // wait for the last schedule to be stopped. --list should list nothing after stopping
            assertListEmpty(cluster.get(1));

            // schedule on the specific table
            cluster.get(1).nodetoolResult("profileload", KEYSPACE, "tbl", "1000", "-i", "1000").asserts().success();
            expectedOutput = String.format("%" + KEYSPACE.length() + "s %5s%n" +
                                           "%s %5s",
                                           "KEYSPACE", "TABLE",
                                           KEYSPACE, "tbl");
            cluster.get(1).nodetoolResult("profileload", "--list")
                   .asserts()
                   .success()
                   .stdoutContains(expectedOutput);
            // stop all should stop the task scheduled with the specific table
            cluster.get(1).nodetoolResult("profileload", "--stop")
                   .asserts().success();
            assertListEmpty(cluster.get(1));
        }
    }

    @Test
    public void testPreventDuplicatedSchedule() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck));"));

            // New sampling; we are good
            cluster.get(1).nodetoolResult("profileload", KEYSPACE, "tbl", "1000", "-i", "1000")
                   .asserts()
                   .success()
                   .stdoutNotContains("Unable to schedule sampling for keyspace");

            // Duplicated sampling (against the same table) but different interval. Nodetool should reject
            cluster.get(1).nodetoolResult("profileload", KEYSPACE, "tbl", "1000", "-i", "1000")
                   .asserts()
                   .success()
                   .stdoutContains("Unable to schedule sampling for keyspace");

            // The "sampling all" request creates overlaps, so it should be rejected too
            cluster.get(1).nodetoolResult("profileload", "1000", "-i", "1000")
                   .asserts()
                   .success()
                   .stdoutContains("Unable to schedule sampling for keyspace");

            cluster.get(1).nodetoolResult("profileload", KEYSPACE, "tbl", "--stop").asserts().success();
            assertListEmpty(cluster.get(1));

            cluster.get(1).nodetoolResult("profileload", "nonexistks", "nonexisttbl", "--stop")
                   .asserts()
                   .success()
                   .stdoutContains("Unable to stop the non-existent scheduled sampling");
        }
    }

    private void assertListEmpty(IInvokableInstance instance)
    {
        Uninterruptibles.sleepUninterruptibly(1500, TimeUnit.MILLISECONDS);
        Assert.assertEquals("--list should list nothing",
                            "KEYSPACE TABLE\n",
                            instance.nodetoolResult("profileload", "--list").getStdout());
    }
}
