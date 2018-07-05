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

package org.apache.cassandra.service.reads.repair;

import java.util.List;

import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.locator.ReplicaLayout;

public class PartitionIteratorMergeListener implements UnfilteredPartitionIterators.MergeListener
{
    private final ReplicaLayout replicaLayout;
    private final ReadCommand command;
    private final ConsistencyLevel consistency;
    private final ReadRepair readRepair;

    public PartitionIteratorMergeListener(ReplicaLayout replicaLayout, ReadCommand command, ConsistencyLevel consistency, ReadRepair readRepair)
    {
        this.replicaLayout = replicaLayout;
        this.command = command;
        this.consistency = consistency;
        this.readRepair = readRepair;
    }

    public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
    {
        return new RowIteratorMergeListener(partitionKey, columns(versions), isReversed(versions), replicaLayout, command, consistency, readRepair);
    }

    protected RegularAndStaticColumns columns(List<UnfilteredRowIterator> versions)
    {
        Columns statics = Columns.NONE;
        Columns regulars = Columns.NONE;
        for (UnfilteredRowIterator iter : versions)
        {
            if (iter == null)
                continue;

            RegularAndStaticColumns cols = iter.columns();
            statics = statics.mergeTo(cols.statics);
            regulars = regulars.mergeTo(cols.regulars);
        }
        return new RegularAndStaticColumns(statics, regulars);
    }

    protected boolean isReversed(List<UnfilteredRowIterator> versions)
    {
        for (UnfilteredRowIterator iter : versions)
        {
            if (iter == null)
                continue;

            // Everything will be in the same order
            return iter.isReverseOrder();
        }

        assert false : "Expected at least one iterator";
        return false;
    }

    public void close()
    {
    }
}

