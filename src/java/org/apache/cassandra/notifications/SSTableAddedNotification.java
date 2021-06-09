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

import javax.annotation.Nullable;

import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.io.sstable.format.SSTableReader;

/**
 * Notification sent after SSTables are added to their {@link org.apache.cassandra.db.ColumnFamilyStore}.
 */
public class SSTableAddedNotification implements INotification
{
    /** The added SSTables */
    public final Iterable<SSTableReader> added;

    /** The memtable from which the tables come when they have been added due to a flush, {@code null} otherwise. */
    @Nullable
    private final Memtable memtable;

    /**
     * Creates a new {@code SSTableAddedNotification} for the specified SSTables and optional memtable.
     *
     * @param added    the added SSTables
     * @param memtable the memtable from which the tables come when they have been added due to a memtable flush,
     *                 or {@code null} if they don't come from a flush
     */
    public SSTableAddedNotification(Iterable<SSTableReader> added, @Nullable Memtable memtable)
    {
        this.added = added;
        this.memtable = memtable;
    }

    /**
     * Returns the memtable from which the tables come when they have been added due to a memtable flush. If not, an
     * empty Optional should be returned.
     *
     * @return the origin memtable in case of a flush, {@link Optional#empty()} otherwise
     */
    public Optional<Memtable> memtable()
    {
        return Optional.ofNullable(memtable);
    }
}
