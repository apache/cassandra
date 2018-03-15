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

package org.apache.cassandra.db;

import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.utils.concurrent.OpOrder;

public class CassandraWriteHandler implements WriteHandler
{
    private final ColumnFamilyStore cfs;

    public CassandraWriteHandler(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
    }

    @Override
    public void apply(PartitionUpdate update, UpdateTransaction indexer, OpOrder.Group opGroup, CommitLogPosition commitLogPosition)
    {
        Memtable mt = cfs.getTracker().getMemtableFor(opGroup, commitLogPosition);
        long timeDelta = mt.put(update, indexer, opGroup);
        // CASSANDRA-11117 - certain resolution paths on memtable put can result in very
        // large time deltas, either through a variety of sentinel timestamps (used for empty values, ensuring
        // a minimal write, etc). This limits the time delta to the max value the histogram
        // can bucket correctly. This also filters the Long.MAX_VALUE case where there was no previous value
        // to update.
        if(timeDelta < Long.MAX_VALUE)
            cfs.metric.colUpdateTimeDeltaHistogram.update(Math.min(18165375903306L, timeDelta));
    }
}
