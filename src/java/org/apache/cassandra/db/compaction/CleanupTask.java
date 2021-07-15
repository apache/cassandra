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

package org.apache.cassandra.db.compaction;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.repair.consistent.admin.CleanupSummary;
import org.apache.cassandra.utils.Pair;

public class CleanupTask
{
    private static final Logger logger = LoggerFactory.getLogger(CleanupTask.class);

    private final ColumnFamilyStore cfs;
    private final List<Pair<UUID, RepairFinishedCompactionTask>> tasks;

    public CleanupTask(ColumnFamilyStore cfs, List<Pair<UUID, RepairFinishedCompactionTask>> tasks)
    {
        this.cfs = cfs;
        this.tasks = tasks;
    }

    public CleanupSummary cleanup()
    {
        Set<UUID> successful = new HashSet<>();
        Set<UUID> unsuccessful = new HashSet<>();
        for (Pair<UUID, RepairFinishedCompactionTask> pair : tasks)
        {
            UUID session = pair.left;
            RepairFinishedCompactionTask task = pair.right;

            if (task != null)
            {
                try
                {
                    task.run();
                    successful.add(session);
                }
                catch (Throwable t)
                {
                    t = task.transaction.abort(t);
                    logger.error("Failed cleaning up " + session, t);
                    unsuccessful.add(session);
                }
            }
            else
            {
                unsuccessful.add(session);
            }
        }
        return new CleanupSummary(cfs, successful, unsuccessful);
    }

    public Throwable abort(Throwable accumulate)
    {
        for (Pair<UUID, RepairFinishedCompactionTask> pair : tasks)
            accumulate = pair.right.transaction.abort(accumulate);
        return accumulate;
    }
}
