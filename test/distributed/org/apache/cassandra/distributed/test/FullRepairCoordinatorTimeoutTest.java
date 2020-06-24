package org.apache.cassandra.distributed.test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairParallelism;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;

@RunWith(Parameterized.class)
public class FullRepairCoordinatorTimeoutTest extends RepairCoordinatorTimeout
{
    public FullRepairCoordinatorTimeoutTest(RepairParallelism parallelism, boolean withNotifications)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-15650
        super(RepairType.FULL, parallelism, withNotifications);
    }
}
