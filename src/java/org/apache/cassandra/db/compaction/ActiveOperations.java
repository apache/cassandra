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
import java.util.concurrent.CopyOnWriteArrayList;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.NonThrowingCloseable;

@ThreadSafe
public class ActiveOperations implements TableOperationObserver
{
    private static final Logger logger = LoggerFactory.getLogger(ActiveOperations.class);

    // The operations ordered by keyspace.table for all the operations that are currently in progress.
    private static final Set<TableOperation> operations = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

    // Keep registered listeners to be called onStart and close
    private final List<CompactionProgressListener> listeners = new CopyOnWriteArrayList<>();

    public interface CompactionProgressListener
    {
        /**
         * Called when compaction started
         */
        default void onStarted(AbstractTableOperation.OperationProgress progress) {}

        /**
         * Called when compaction completed
         */
        default void onCompleted(AbstractTableOperation.OperationProgress progressOnCompleted) {}
    }

    public void registerListener(CompactionProgressListener listener)
    {
        listeners.add(listener);
    }

    public void unregisterListener(CompactionProgressListener listener)
    {
        listeners.remove(listener);
    }

    /**
     * @return all the table operations currently in progress. This is mostly compactions but it can include other
     *         operations too, basically any operation that calls {@link this#onOperationStart(TableOperation).}
     */
    public List<TableOperation> getTableOperations()
    {
        ImmutableList.Builder<TableOperation> builder = ImmutableList.builder();
        synchronized (operations)
        {
            builder.addAll(operations);
        }
        return builder.build();
    }

    @Override
    public NonThrowingCloseable onOperationStart(TableOperation op)
    {
        AbstractTableOperation.OperationProgress progress = op.getProgress();
        for (CompactionProgressListener listener : listeners)
        {
            try
            {
                listener.onStarted(progress);
            }
            catch (Throwable t)
            {
                String listenerName = listener.getClass().getName();
                logger.error("Unable to notify listener {} while trying to start compaction {} on table {}",
                             listenerName, progress.operationType(), progress.metadata(), t);
            }
        }
        operations.add(op);
        return () -> {
            operations.remove(op);
            AbstractTableOperation.OperationProgress progressOnCompleted = op.getProgress();
            CompactionManager.instance.getMetrics().bytesCompacted.inc(progressOnCompleted.total());
            CompactionManager.instance.getMetrics().totalCompactionsCompleted.mark();

            for (CompactionProgressListener listener : listeners)
            {
                try
                {
                    listener.onCompleted(progressOnCompleted);
                }
                catch (Throwable t)
                {
                    String listenerName = listener.getClass().getName();
                    logger.error("Unable to notify listener {} while trying to complete compaction {} on table {}",
                                 listenerName, progressOnCompleted.operationType(), progressOnCompleted.metadata(), t);
                }
            }
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
