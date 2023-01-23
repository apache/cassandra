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

package org.apache.cassandra.repair;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.repair.state.JobState;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

public abstract class AbstractRepairJob extends AsyncFuture<RepairResult> implements Runnable
{
    public final JobState state;
    protected final RepairJobDesc desc;
    protected final RepairSession session;
    protected final ExecutorPlus taskExecutor;

    protected final Keyspace ks;
    protected final ColumnFamilyStore cfs;

    public AbstractRepairJob(RepairSession session, String columnFamily)
    {
        this.session = session;
        this.taskExecutor = session.taskExecutor;
        this.desc = new RepairJobDesc(session.state.parentRepairSession, session.getId(), session.state.keyspace, columnFamily, session.state.commonRange.ranges);
        this.state = new JobState(desc, session.state.commonRange.endpoints);
        this.ks = Keyspace.open(desc.keyspace);
        this.cfs = ks.getColumnFamilyStore(desc.columnFamily);
    }

    public void run()
    {
        state.phase.start();
        cfs.metric.repairsStarted.inc();
        runRepair();
    }

    abstract protected void runRepair();
}
