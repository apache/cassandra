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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.NonThrowingCloseable;

public class ActiveOperations implements TableOperationObserver
{
    // The operations ordered by keyspace.table for all the operations that are currently in progress.
    private static final Set<TableOperation> operations = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

    /**
     * @return all the table operations currently in progress. This is mostly compactions but it can include other
     *         operations too, basically any operation that calls {@link this#onOperationStart(TableOperation).}
     */
    public List<TableOperation> getTableOperations()
    {
        ImmutableList.Builder<TableOperation> builder = ImmutableList.builder();
        builder.addAll(operations);
        return builder.build();
    }

    @Override
    public NonThrowingCloseable onOperationStart(TableOperation op)
    {
            operations.add(op);
            return () -> {
                operations.remove(op);
                TableOperation.Progress progress = op.getProgress();
                CompactionManager.instance.getMetrics().bytesCompacted.inc(progress.total());
                CompactionManager.instance.getMetrics().totalCompactionsCompleted.mark();
            };
    }

    /**
     * Iterates over the active operations and tries to find OperationProgresses with the given operation type for the given sstable
     *
     * Number of entries in operations should be small (< 10) but avoid calling in any time-sensitive context
     */
    public Collection<AbstractTableOperation.OperationProgress> getOperationsForSSTable(SSTableReader sstable, OperationType operationType)
    {
        List<AbstractTableOperation.OperationProgress> toReturn = null;

        synchronized (operations)
        {
            for (TableOperation op : operations)
            {
                AbstractTableOperation.OperationProgress progress = op.getProgress();
                if (progress.sstables().contains(sstable) && progress.operationType() == operationType)
                {
                    if (toReturn == null)
                        toReturn = new ArrayList<>();
                    toReturn.add(progress);
                }
            }
        }
        return toReturn;
    }

    /**
     * @return true if given table operation is still active
     */
    public boolean isActive(TableOperation op)
    {
        return getTableOperations().contains(op);
    }
}
