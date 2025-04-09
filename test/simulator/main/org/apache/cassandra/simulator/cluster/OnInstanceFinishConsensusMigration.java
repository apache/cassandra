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

package org.apache.cassandra.simulator.cluster;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tools.RepairRunner;
import org.apache.cassandra.tools.RepairRunner.RepairCmd;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.simulator.Action.Modifiers.NONE;
import static org.apache.cassandra.simulator.Action.Modifiers.RELIABLE_NO_TIMEOUTS;

public class OnInstanceFinishConsensusMigration extends ClusterAction
{
    public OnInstanceFinishConsensusMigration(KeyspaceActions actions, int on, String range, String target)
    {
        super("Finish consensus migration  on " + on + " " + actions.keyspace + "." + actions.table + " " + range, modeForTarget(target), modeForTarget(target), actions, on, invokableBlockingFinishConsensusMigration(actions.keyspace, actions.table, range, target));
    }

    private static Action.Modifiers modeForTarget(String target)
    {
        if (target.equals("off"))
            return RELIABLE_NO_TIMEOUTS;
        return NONE;
    }
    private static IIsolatedExecutor.SerializableRunnable invokableBlockingFinishConsensusMigration(String keyspaceName, String table, String range, String target)
    {
        return () -> {
            try (RepairRunner runner = new RepairRunner(System.out, null, StorageService.instance, new RepairCmd(keyspaceName)
            {
                @Override
                public Integer start()
                {
                    Epoch initialEpoch = ClusterMetadata.current().epoch;
                    Integer cmd = StorageService.instance.finishConsensusMigration(keyspaceName, ImmutableList.of(table), range, target);
                    // This isn't going to work well if there are other sources of epoch changes
                    // but just to start it addresses the issue that TCM hasn't updated on this node yet
                    if (cmd == null)
                    {
                        ClusterMetadataService.instance().fetchLogFromCMS(initialEpoch.nextEpoch());
                        cmd = StorageService.instance.finishConsensusMigration(keyspaceName, ImmutableList.of(table), range, target);
                        checkState(cmd != null);
                    }
                    return cmd;
                }
            }))
            {
                runner.start();
                try
                {
                    runner.run();
                }
                catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
