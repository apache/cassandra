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

import java.time.Duration;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.LongTokenRange;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.api.NodeToolResult.ProgressEventType;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairParallelism;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.IMessageFilters.Matcher.of;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairFailedWithMessageContains;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairNotExist;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairSuccess;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.getRepairExceptions;
import static org.apache.cassandra.utils.AssertUtil.assertTimeoutPreemptively;

public abstract class RepairCoordinatorFast extends RepairCoordinatorBase
{
    public RepairCoordinatorFast(RepairType repairType, RepairParallelism parallelism, boolean withNotifications)
    {
        super(repairType, parallelism, withNotifications);
    }

    @Test
    public void simple() {
        String table = tableName("simple");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, PRIMARY KEY (key))", KEYSPACE, table));
            CLUSTER.coordinator(1).execute(format("INSERT INTO %s.%s (key) VALUES (?)", KEYSPACE, table), ConsistencyLevel.ANY, "some text");

            long repairExceptions = getRepairExceptions(CLUSTER, 2);
            NodeToolResult result = repair(2, KEYSPACE, table);
            result.asserts().success();
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(ProgressEventType.START, "Starting repair command")
                      .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(ProgressEventType.SUCCESS, repairType != RepairType.PREVIEW ? "Repair completed successfully": "Repair preview completed successfully")
                      .notificationContains(ProgressEventType.COMPLETE, "finished");
            }

            if (repairType != RepairType.PREVIEW)
            {
                assertParentRepairSuccess(CLUSTER, KEYSPACE, table);
            }
            else
            {
                assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
            }

            Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 2));
        });
    }

    @Test
    public void missingKeyspace()
    {
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            // as of this moment the check is done in nodetool so the JMX notifications are not imporant
            // nor is the history stored
            long repairExceptions = getRepairExceptions(CLUSTER, 2);
            NodeToolResult result = repair(2, "doesnotexist");
            result.asserts()
                  .failure()
                  .errorContains("Keyspace [doesnotexist] does not exist.");

            Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 2));

            assertParentRepairNotExist(CLUSTER, "doesnotexist");
        });
    }

    @Test
    public void missingTable()
    {
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            long repairExceptions = getRepairExceptions(CLUSTER, 2);
            String tableName = tableName("doesnotexist");
            NodeToolResult result = repair(2, KEYSPACE, tableName);
            result.asserts()
                  .failure();
            if (withNotifications)
            {
                result.asserts()
                      .errorContains("Unknown keyspace/cf pair (distributed_test_keyspace." + tableName + ")")
                      // Start notification is ignored since this is checked during setup (aka before start)
                      .notificationContains(ProgressEventType.ERROR, "failed with error Unknown keyspace/cf pair (distributed_test_keyspace." + tableName + ")")
                      .notificationContains(ProgressEventType.COMPLETE, "finished with error");
            }

            assertParentRepairNotExist(CLUSTER, KEYSPACE, "doesnotexist");

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 2));
        });
    }

    @Test
    public void noTablesToRepair()
    {
        // index CF currently don't support repair, so they get dropped when listed
        // this is done in this test to cause the keyspace to have 0 tables to repair, which causes repair to no-op
        // early and skip.
        String table = tableName("withindex");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
            CLUSTER.schemaChange(format("CREATE INDEX value_%s ON %s.%s (value)", postfix(), KEYSPACE, table));

            long repairExceptions = getRepairExceptions(CLUSTER, 2);
            // if CF has a . in it, it is assumed to be a 2i which rejects repairs
            NodeToolResult result = repair(2, KEYSPACE, table + ".value");
            result.asserts().success();
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains("Empty keyspace")
                      .notificationContains("skipping repair: " + KEYSPACE)
                      // Start notification is ignored since this is checked during setup (aka before start)
                      .notificationContains(ProgressEventType.SUCCESS, "Empty keyspace") // will fail since success isn't returned; only complete
                      .notificationContains(ProgressEventType.COMPLETE, "finished"); // will fail since it doesn't do this
            }

            assertParentRepairNotExist(CLUSTER, KEYSPACE, table + ".value");

            // this is actually a SKIP and not a FAILURE, so shouldn't increment
            Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 2));
        });
    }

    @Test
    public void intersectingRange()
    {
        // this test exists to show that this case will cause repair to finish; success or failure isn't imporant
        // if repair is enhanced to allow intersecting ranges w/ local then this test will fail saying that we expected
        // repair to fail but it didn't, this would be fine and this test should be updated to reflect the new
        // semantic
        String table = tableName("intersectingrange");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));

            //TODO dtest api for this?
            LongTokenRange tokenRange = CLUSTER.get(2).callOnInstance(() -> {
                Set<Range<Token>> ranges = StorageService.instance.getLocalReplicas(KEYSPACE).ranges();
                Range<Token> range = Iterables.getFirst(ranges, null);
                long left = (long) range.left.getTokenValue();
                long right = (long) range.right.getTokenValue();
                return new LongTokenRange(left, right);
            });
            LongTokenRange intersectingRange = new LongTokenRange(tokenRange.maxInclusive - 7, tokenRange.maxInclusive + 7);

            long repairExceptions = getRepairExceptions(CLUSTER, 2);
            NodeToolResult result = repair(2, KEYSPACE, table,
                                           "--start-token", Long.toString(intersectingRange.minExclusive),
                                           "--end-token", Long.toString(intersectingRange.maxInclusive));
            result.asserts()
                  .failure()
                  .errorContains("Requested range " + intersectingRange + " intersects a local range (" + tokenRange + ") but is not fully contained in one");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(ProgressEventType.START, "Starting repair command")
                      .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(ProgressEventType.ERROR, "Requested range " + intersectingRange + " intersects a local range (" + tokenRange + ") but is not fully contained in one")
                      .notificationContains(ProgressEventType.COMPLETE, "finished with error");
            }

            assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 2));
        });
    }

    @Test
    public void unknownHost()
    {
        String table = tableName("unknownhost");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));

            long repairExceptions = getRepairExceptions(CLUSTER, 2);
            NodeToolResult result = repair(2, KEYSPACE, table, "--in-hosts", "thisreally.should.not.exist.apache.org");
            result.asserts()
                  .failure()
                  .errorContains("Unknown host specified thisreally.should.not.exist.apache.org");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(ProgressEventType.START, "Starting repair command")
                      .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(ProgressEventType.ERROR, "Unknown host specified thisreally.should.not.exist.apache.org")
                      .notificationContains(ProgressEventType.COMPLETE, "finished with error");
            }

            assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 2));
        });
    }

    @Test
    public void desiredHostNotCoordinator()
    {
        // current limitation is that the coordinator must be apart of the repair, so as long as that exists this test
        // verifies that the validation logic will termniate the repair properly
        String table = tableName("desiredhostnotcoordinator");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));

            long repairExceptions = getRepairExceptions(CLUSTER, 2);
            NodeToolResult result = repair(2, KEYSPACE, table, "--in-hosts", "localhost");
            result.asserts()
                  .failure()
                  .errorContains("The current host must be part of the repair");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(ProgressEventType.START, "Starting repair command")
                      .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(ProgressEventType.ERROR, "The current host must be part of the repair")
                      .notificationContains(ProgressEventType.COMPLETE, "finished with error");
            }

            assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 2));
        });
    }

    @Test
    public void onlyCoordinator()
    {
        // this is very similar to ::desiredHostNotCoordinator but has the difference that the only host to do repair
        // is the coordinator
        String table = tableName("onlycoordinator");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));

            long repairExceptions = getRepairExceptions(CLUSTER, 2);
            NodeToolResult result = repair(1, KEYSPACE, table, "--in-hosts", "localhost");
            result.asserts()
                  .failure()
                  .errorContains("Specified hosts [localhost] do not share range");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(ProgressEventType.START, "Starting repair command")
                      .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(ProgressEventType.ERROR, "Specified hosts [localhost] do not share range")
                      .notificationContains(ProgressEventType.COMPLETE, "finished with error");
            }

            assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

            //TODO should this be marked as fail to match others?  Should they not be marked?
            Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 2));
        });
    }

    @Test
    public void replicationFactorOne()
    {
        // In the case of rf=1 repair fails to create a cmd handle so node tool exists early
        String table = tableName("one");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            // since cluster is shared and this test gets called multiple times, need "IF NOT EXISTS" so the second+ attempt
            // does not fail
            CLUSTER.schemaChange("CREATE KEYSPACE IF NOT EXISTS replicationfactor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
            CLUSTER.schemaChange(format("CREATE TABLE replicationfactor.%s (key text, value text, PRIMARY KEY (key))", table));

            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, "replicationfactor", table);
            result.asserts()
                  .success();

            assertParentRepairNotExist(CLUSTER, KEYSPACE, table);

            Assert.assertEquals(repairExceptions, getRepairExceptions(CLUSTER, 1));
        });
    }

    @Test
    public void prepareFailure()
    {
        String table = tableName("preparefailure");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
            IMessageFilters.Filter filter = CLUSTER.verbs(Verb.PREPARE_MSG).messagesMatching(of(m -> {
                throw new RuntimeException("prepare fail");
            })).drop();
            try
            {
                long repairExceptions = getRepairExceptions(CLUSTER, 1);
                NodeToolResult result = repair(1, KEYSPACE, table);
                result.asserts()
                      .failure()
                      .errorContains("Got negative replies from endpoints");
                if (withNotifications)
                {
                    result.asserts()
                          .notificationContains(ProgressEventType.START, "Starting repair command")
                          .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                          .notificationContains(ProgressEventType.ERROR, "Got negative replies from endpoints")
                          .notificationContains(ProgressEventType.COMPLETE, "finished with error");
                }

                Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
                if (repairType != RepairType.PREVIEW)
                {
                    assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Got negative replies from endpoints");
                }
                else
                {
                    assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
                }
            }
            finally
            {
                filter.off();
            }
        });
    }

    @Test
    public void snapshotFailure()
    {
        Assume.assumeFalse("incremental does not do snapshot", repairType == RepairType.INCREMENTAL);
        Assume.assumeFalse("Parallel repair does not perform snapshots", parallelism == RepairParallelism.PARALLEL);

        String table = tableName("snapshotfailure");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
            IMessageFilters.Filter filter = CLUSTER.verbs(Verb.SNAPSHOT_MSG).messagesMatching(of(m -> {
                throw new RuntimeException("snapshot fail");
            })).drop();
            try
            {
                long repairExceptions = getRepairExceptions(CLUSTER, 1);
                NodeToolResult result = repair(1, KEYSPACE, table);
                result.asserts()
                      .failure();
                      // Right now coordination doesn't propgate the first exception, so we only know "there exists a issue".
                      // With notifications on nodetool will see the error then complete, so the cmd state (what nodetool
                      // polls on) is ignored.  With notifications off or dropped, the poll await fails and queries cmd
                      // state, and that will have the below error.
                      // NOTE: this isn't desireable, would be good to propgate
                      // TODO replace with errorContainsAny once dtest api updated
                Throwable error = result.getError();
                Assert.assertNotNull("Error was null", error);
                if (!(error.getMessage().contains("Could not create snapshot") || error.getMessage().contains("Some repair failed")))
                    throw new AssertionError("Unexpected error, expected to contain 'Could not create snapshot' or 'Some repair failed'", error);
                if (withNotifications)
                {
                    result.asserts()
                          .notificationContains(ProgressEventType.START, "Starting repair command")
                          .notificationContains(ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                          .notificationContains(ProgressEventType.ERROR, "Could not create snapshot ")
                          .notificationContains(ProgressEventType.COMPLETE, "finished with error");
                }

                Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
                if (repairType != RepairType.PREVIEW)
                {
                    assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Could not create snapshot");
                }
                else
                {
                    assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
                }
            }
            finally
            {
                filter.off();
            }
        });
    }
}
