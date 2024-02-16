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

package org.apache.cassandra.service.accord.repair;

import java.math.BigInteger;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.repair.AbstractRepairJob;
import org.apache.cassandra.repair.RepairResult;
import org.apache.cassandra.repair.RepairSession;
import org.apache.cassandra.service.consensus.migration.ConsensusMigrationRepairResult;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;

import static java.util.Collections.emptyList;

public class AccordRepairJob extends AbstractRepairJob implements AccordRepair.Listener
{
    private static final Logger logger = LoggerFactory.getLogger(AccordRepair.class);

    public static final BigInteger TWO = BigInteger.valueOf(2);

    private BigInteger rangeStep;

    private Epoch minEpoch = ClusterMetadata.current().epoch;

    private final AccordRepair repair;
    private volatile long barrierStart;

    public AccordRepairJob(RepairSession repairSession, String cfname)
    {
        super(repairSession, cfname);
        IPartitioner partitioner = desc.ranges.iterator().next().left.getPartitioner();
        this.repair = new AccordRepair(this, partitioner, desc.keyspace, desc.ranges);
    }

    @Override
    protected void runRepair()
    {
        try
        {
            repair.repair();
            state.phase.success();
            cfs.metric.repairsCompleted.inc();
            trySuccess(new RepairResult(desc, emptyList(), ConsensusMigrationRepairResult.fromAccordRepair(minEpoch)));
        }
        catch (Throwable t)
        {
            state.phase.fail(t);
            cfs.metric.repairsCompleted.inc();
            tryFailure(t);
        }
    }

    @Override
    protected void abort(@Nullable Throwable reason)
    {
        repair.abort(reason);
    }

    @Override
    public void onBarrierStart()
    {
        barrierStart = ctx.clock().nanoTime();
    }

    @Override
    public void onBarrierException(Throwable throwable)
    {
        if (throwable instanceof RuntimeException)
        {
            // TODO Placeholder for dependency limit overflow
            cfs.metric.rangeMigrationDependencyLimitFailures.mark();
        }
        else
        {
            // unexpected error
            cfs.metric.rangeMigrationUnexpectedFailures.mark();
        }
    }

    @Override
    public void onBarrierComplete()
    {
        cfs.metric.rangeMigration.addNano(barrierStart);
    }
}
