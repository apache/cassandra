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
                  .errorContains("Got negative replies from endpoints [127.0.0.2:7012]");
            if (withNotifications)
            {
                result.asserts()
                      .notificationContains(NodeToolResult.ProgressEventType.START, "Starting repair command")
                      .notificationContains(NodeToolResult.ProgressEventType.START, "repairing keyspace " + KEYSPACE + " with repair options")
                      .notificationContains(NodeToolResult.ProgressEventType.ERROR, "Got negative replies from endpoints [127.0.0.2:7012]")
                      .notificationContains(NodeToolResult.ProgressEventType.COMPLETE, "finished with error");
            }

            if (repairType != RepairType.PREVIEW)
            {
                assertParentRepairFailedWithMessageContains(CLUSTER, KEYSPACE, table, "Got negative replies from endpoints [127.0.0.2:7012]");
            }
            else
            {
                assertParentRepairNotExist(CLUSTER, KEYSPACE, table);
            }

            Assert.assertEquals(repairExceptions + 1, getRepairExceptions(CLUSTER, 1));
        });
    }
}
