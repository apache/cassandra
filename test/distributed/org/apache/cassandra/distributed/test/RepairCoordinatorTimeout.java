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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairParallelism;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;
import org.apache.cassandra.net.Verb;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairFailedWithMessageContains;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.assertParentRepairNotExist;
import static org.apache.cassandra.distributed.test.DistributedRepairUtils.getRepairExceptions;
import static org.apache.cassandra.utils.AssertUtil.assertTimeoutPreemptively;

public abstract class RepairCoordinatorTimeout extends RepairCoordinatorBase
{
    public RepairCoordinatorTimeout(RepairType repairType, RepairParallelism parallelism, boolean withNotifications)
    {
        super(repairType, parallelism, withNotifications);
    }

    @Before
    public void beforeTest()
    {
        CLUSTER.filters().reset();
    }

    @Test
    public void prepareRPCTimeout()
    {
        String table = tableName("preparerpctimeout");
        assertTimeoutPreemptively(Duration.ofMinutes(1), () -> {
            CLUSTER.schemaChange(format("CREATE TABLE %s.%s (key text, value text, PRIMARY KEY (key))", KEYSPACE, table));
            CLUSTER.verbs(Verb.PREPARE_MSG).drop();

            long repairExceptions = getRepairExceptions(CLUSTER, 1);
            NodeToolResult result = repair(1, KEYSPACE, table);
            result.asserts()
                  .failure()
                  .errorContains("Did not get replies from all endpoints.");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(NodeToolResult.ProgressEventType.START, "Starting repair command")
                      .notificationContains(NodeToolResult.ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Did not get replies from all endpoints.")
                      .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
            }

            if (repairType != RepairType.PREVIEW)
            {
                assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Did not get replies from all endpoints.");
            }
            else
            {
                assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
            }

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
        });
    }
}
