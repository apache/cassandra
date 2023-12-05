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
package org.apache.cassandra.notifications;

import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Notification sent before SSTables are added to their {@link org.apache.cassandra.db.ColumnFamilyStore}.
 */
public class SSTableAddingNotification implements INotification
{
    /** The SSTables to be added*/
    public final Iterable<SSTableReader> adding;

    /** The memtable from which the sstables come when they need to be added due to a flush, {@code null} otherwise. */
    @Nullable
    private final Memtable memtable;

    /** The type of operation that created the sstables */
    public final OperationType operationType;

    /** The id of the operation that created the sstables, if available */
    public final Optional<UUID> operationId;

    /**
     * Creates a new {@code SSTableAddingNotification} for the specified SSTables and optional memtable.
     *
     * @param adding    the SSTables to be added
     * @param memtable the memtable from which the sstables come when they need to be added due to a memtable flush,
     *                 or {@code null} if they don't come from a flush
     * @param operationType the type of operation that created the sstables
     * @param operationId the id of the operation (transaction) that created the sstables, or empty if no id is available
     */
    public SSTableAddingNotification(Iterable<SSTableReader> adding, @Nullable Memtable memtable, OperationType operationType, Optional<UUID> operationId)
    {
        this.adding = adding;
        this.memtable = memtable;
        this.operationType = operationType;
        this.operationId = operationId;
    }

    /**
     * Returns the memtable from which the sstables come when they need to be addeddue to a memtable flush. If not, an
     * empty Optional should be returned.
     *
     * @return the origin memtable in case of a flush, {@link Optional#empty()} otherwise
     */
    public Optional<Memtable> memtable()
    {
        return Optional.ofNullable(memtable);
    }
}
