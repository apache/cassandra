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

package org.apache.cassandra.locator;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.tcm.ClusterMetadata;

public class CoordinationPlanTestUtils
{
    private static ResponseTracker createTrackerForWrite(ReplicaPlan.ForWrite plan)
    {
        return plan.replicationStrategy().createTrackerForWrite(plan.consistencyLevel(), plan, plan.pending, ClusterMetadata.current());
    }

    public static CoordinationPlan.ForWriteWithIdeal create(ReplicaPlan.ForWrite plan, ConsistencyLevel idealCL)
    {
        ResponseTracker tracker = createTrackerForWrite(plan);

        CoordinationPlan.ForWrite idealPlan = null;
        if (idealCL != null && idealCL != plan.consistencyLevel())
        {
            ReplicaPlan.ForWrite idealReplicaPlan = plan.withConsistencyLevel(idealCL);
            ResponseTracker idealTracker = createTrackerForWrite(idealReplicaPlan);
            idealPlan = new CoordinationPlan.ForWrite(idealReplicaPlan, idealTracker);
        }

        return new CoordinationPlan.ForWriteWithIdeal(ClusterMetadata.current(), plan, tracker, idealPlan);
    }

    public static CoordinationPlan.ForTokenRead create(ReplicaPlan.ForTokenRead plan)
    {
        return new CoordinationPlan.ForTokenRead(ReplicaPlan.shared(plan), trackerForRead(plan));
    }

    public static CoordinationPlan.ForTokenRead create(ReplicaPlan.SharedForTokenRead shared)
    {
        return new CoordinationPlan.ForTokenRead(shared, trackerForRead(shared.get()));
    }

    public static CoordinationPlan.ForRangeRead create(ReplicaPlan.ForRangeRead plan)
    {
        return new CoordinationPlan.ForRangeRead(ReplicaPlan.shared(plan), trackerForRead(plan));
    }

    public static CoordinationPlan.ForRangeRead create(ReplicaPlan.SharedForRangeRead shared)
    {
        return new CoordinationPlan.ForRangeRead(shared, trackerForRead(shared.get()));
    }

    private static <E extends Endpoints<E>, P extends ReplicaPlan.ForRead<E, P>> ResponseTracker trackerForRead(P plan)
    {
        return new SimpleResponseTracker(plan.readQuorum(), plan.readCandidates().size());
    }
}
