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

import java.util.function.Function;

import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.simulator.Action;
import org.apache.cassandra.simulator.ActionList;
import org.apache.cassandra.tcm.ClusterMetadataService;

/**
 * Waits for a node to be fully caught up with the latest changes known to CMS
 */
public class Quiesce extends ClusterReliableAction
{
    Quiesce(ClusterActions actions, int on)
    {
        super("Quiesce " + on, actions, on, invokableQuiesce());
    }

    public static ActionList all(ClusterActions actions)
    {
        return actions.onAll(Quiesce.factory(actions));
    }

    public static Function<Integer, Action> factory(ClusterActions actions)
    {
        return (on) -> new Quiesce(actions, on);
    }

    private static IIsolatedExecutor.SerializableRunnable invokableQuiesce()
    {
        return () -> {
            ClusterMetadataService.instance().processor().fetchLogAndWait();
            ClusterMetadataService.instance().log().waitForHighestConsecutive();
        };
    }
}
