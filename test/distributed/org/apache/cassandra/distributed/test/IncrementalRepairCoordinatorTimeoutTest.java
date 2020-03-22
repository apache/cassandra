package org.apache.cassandra.distributed.test;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairParallelism;
import org.apache.cassandra.distributed.test.DistributedRepairUtils.RepairType;

@RunWith(Parameterized.class)
public class IncrementalRepairCoordinatorTimeoutTest extends RepairCoordinatorTimeout
{
    public IncrementalRepairCoordinatorTimeoutTest(RepairParallelism parallelism, boolean withNotifications)
    {
        super(RepairType.INCREMENTAL, parallelism, withNotifications);
    }
}
