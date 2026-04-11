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

package org.apache.cassandra.simulator;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.simulator.cluster.ClusterActions;
import org.apache.cassandra.simulator.systems.SimulatedSystems;
import org.apache.cassandra.utils.CloseableIterator;

public abstract class AbstractSimulation implements Simulation
{
    public final SimulatedSystems simulated;
    public final RunnableActionScheduler scheduler;
    public final Cluster cluster;
    public final ClusterActions clusterActions;

    protected AbstractSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster)
    {
        this(simulated, scheduler, cluster, ClusterActions.simple(simulated, cluster));
    }

    protected AbstractSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions.Options options)
    {
        this(simulated, scheduler, cluster, ClusterActions.simple(simulated, cluster, options));
    }

    protected AbstractSimulation(SimulatedSystems simulated, RunnableActionScheduler scheduler, Cluster cluster, ClusterActions clusterActions)
    {
        this.simulated = simulated;
        this.scheduler = scheduler;
        this.cluster = cluster;
        this.clusterActions = clusterActions;
    }

    @Override
    public void run()
    {
        try (CloseableIterator<?> iter = iterator())
        {
            while (iter.hasNext())
            {
                checkForErrors();
                iter.next();
            }
            checkForErrors();
        }
    }

    protected void checkForErrors()
    {
        if (simulated.failures.hasFailure())
        {
            AssertionError error = new AssertionError("Errors detected during simulation");
            // don't care about the stack trace... the issue is the errors found and not what part of the scheduler we stopped
            error.setStackTrace(new StackTraceElement[0]);
            simulated.failures.get().forEach(error::addSuppressed);
            throw error;
        }
    }

    @Override
    public void close() throws Exception
    {

    }
}
